package com.gitster.dj

import android.content.Intent
import android.net.Uri
import android.widget.Toast
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.keyframes
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.Saver
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.launch

private sealed interface Screen {
    data object Home : Screen
    data object Scan : Screen
    data class Playback(val playable: PlayableCard) : Screen
}

data class PlayableCard(
    val raw: String,
    val kind: ScanResolution.Kind,
    val cardId: String?,
    val trackId: String?,
    val spotifyUri: String,
    val title: String? = null,
    val artists: String? = null,
    val year: Int? = null
)

private val ScreenSaver: Saver<Screen, Any> = Saver(
    save = { s ->
        when (s) {
            Screen.Home -> arrayListOf("home")
            Screen.Scan -> arrayListOf("scan")
            is Screen.Playback -> arrayListOf(
                "playback",
                s.playable.raw,
                s.playable.kind.name,
                s.playable.cardId.orEmpty(),
                s.playable.trackId.orEmpty(),
                s.playable.spotifyUri,
                s.playable.title.orEmpty(),
                s.playable.artists.orEmpty(),
                s.playable.year?.toString().orEmpty()
            )
        }
    },
    restore = { restored ->
        val list = restored as? List<*> ?: return@Saver Screen.Home
        when (list.getOrNull(0) as? String ?: "home") {
            "scan" -> Screen.Scan
            "playback" -> {
                val raw = list.getOrNull(1) as? String ?: ""
                val kindName = list.getOrNull(2) as? String ?: ScanResolution.Kind.UNKNOWN.name
                val kind = runCatching { ScanResolution.Kind.valueOf(kindName) }
                    .getOrDefault(ScanResolution.Kind.UNKNOWN)
                val cardId = (list.getOrNull(3) as? String).orEmpty().ifBlank { null }
                val trackId = (list.getOrNull(4) as? String).orEmpty().ifBlank { null }
                val spotifyUri = list.getOrNull(5) as? String ?: ""
                val title = (list.getOrNull(6) as? String).orEmpty().ifBlank { null }
                val artists = (list.getOrNull(7) as? String).orEmpty().ifBlank { null }
                val year = (list.getOrNull(8) as? String).orEmpty().ifBlank { null }?.toIntOrNull()

                Screen.Playback(
                    playable = PlayableCard(
                        raw = raw,
                        kind = kind,
                        cardId = cardId,
                        trackId = trackId,
                        spotifyUri = spotifyUri,
                        title = title,
                        artists = artists,
                        year = year
                    )
                )
            }
            else -> Screen.Home
        }
    }
)

@Composable
fun GitsterApp(
    repo: DeckRepository,
    rulesUrl: String
) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()

    var screen by rememberSaveable(stateSaver = ScreenSaver) { mutableStateOf<Screen>(Screen.Home) }
    var deckLoaded by remember { mutableStateOf(false) }
    var loading by remember { mutableStateOf(false) }

    LaunchedEffect(Unit) {
        runCatching { repo.loadDeckIfNeeded() }
        deckLoaded = true
    }

    Surface(modifier = Modifier.fillMaxSize()) {
        Box(Modifier.fillMaxSize()) {
            when (val s = screen) {
                Screen.Home -> HomeScreen(
                    onPlayNow = { screen = Screen.Scan },
                    onRules = {
                        runCatching {
                            context.startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(rulesUrl)))
                        }.onFailure {
                            Toast.makeText(context, "No puedo abrir el enlace", Toast.LENGTH_SHORT).show()
                        }
                    }
                )

                Screen.Scan -> ScanScreen(
                    onClose = { screen = Screen.Home },
                    onScanned = { raw ->
                        if (loading) return@ScanScreen
                        loading = true
                        scope.launch {
                            try {
                                if (!deckLoaded) {
                                    runCatching { repo.loadDeckIfNeeded() }
                                    deckLoaded = true
                                }

                                val res = repo.resolveFromRaw(rawInput = raw)
                                val card = res.card
                                if (card == null) {
                                    Toast.makeText(context, "No encontrada", Toast.LENGTH_SHORT).show()
                                } else {
                                    val uri = bestSpotifyUriString(res, card)
                                    if (uri == null) {
                                        Toast.makeText(context, "Carta encontrada pero sin Spotify URI", Toast.LENGTH_SHORT).show()
                                    } else {
                                        screen = Screen.Playback(
                                            PlayableCard(
                                                raw = res.raw,
                                                kind = res.kind,
                                                cardId = res.parsedCardId ?: card.cardId,
                                                trackId = res.parsedTrackId ?: card.trackId,
                                                spotifyUri = uri,
                                                title = card.titleForUi(),
                                                artists = card.artistsForUi(),
                                                year = card.yearAsIntOrNull()
                                            )
                                        )
                                    }
                                }
                            } catch (t: Throwable) {
                                Toast.makeText(context, t.message ?: "Error resolviendo", Toast.LENGTH_SHORT).show()
                            } finally {
                                loading = false
                            }
                        }
                    }
                )

                is Screen.Playback -> PlaybackScreen(
                    playable = s.playable,
                    onNextCard = { screen = Screen.Scan },
                    onBackHome = { screen = Screen.Home }
                )
            }

            if (loading) {
                Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                    Column(horizontalAlignment = Alignment.CenterHorizontally) {
                        CircularProgressIndicator()
                        Spacer(Modifier.height(10.dp))
                        Text("Cargando / resolviendo…")
                    }
                }
            }
        }
    }
}

@Composable
private fun HomeScreen(
    onPlayNow: () -> Unit,
    onRules: () -> Unit
) {
    val shapeLg = RoundedCornerShape(20.dp)
    val shapePill = RoundedCornerShape(999.dp)

    Box(modifier = Modifier.fillMaxSize()) {
        Image(
            painter = painterResource(id = R.drawable.home_bg),
            contentDescription = null,
            modifier = Modifier.fillMaxSize(),
            contentScale = ContentScale.Crop
        )

        Box(
            modifier = Modifier
                .fillMaxSize()
                .background(
                    Brush.verticalGradient(
                        0f to Color(0x88060814),
                        0.55f to Color(0x99060814),
                        1f to Color(0xBB060814)
                    )
                )
        )

        Column(
            modifier = Modifier
                .fillMaxSize()
                .safeDrawingPadding()
                .padding(horizontal = 18.dp, vertical = 16.dp),
            verticalArrangement = Arrangement.Center,
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Column(Modifier.fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally) {
                Spacer(Modifier.height(6.dp))
                NeonFlickerLogo(modifier = Modifier.fillMaxWidth())

                Spacer(Modifier.height(18.dp))

                Button(
                    onClick = onPlayNow,
                    modifier = Modifier.fillMaxWidth().height(58.dp),
                    shape = shapeLg,
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.secondary,
                        contentColor = Color.Black
                    )
                ) {
                    Text(
                        "JUGAR AHORA",
                        fontWeight = FontWeight.Black,
                        style = MaterialTheme.typography.titleMedium
                    )
                }

                Spacer(Modifier.height(12.dp))

                OutlinedButton(
                    onClick = onRules,
                    shape = shapePill,
                    modifier = Modifier.fillMaxWidth().height(40.dp),
                    colors = ButtonDefaults.outlinedButtonColors(
                        contentColor = MaterialTheme.colorScheme.primary
                    ),
                    border = androidx.compose.foundation.BorderStroke(
                        1.dp,
                        MaterialTheme.colorScheme.primary
                    )
                ) {
                    Text("REGLAS", fontWeight = FontWeight.SemiBold)
                }
            }
        }
    }
}

@Composable
private fun NeonFlickerLogo(modifier: Modifier = Modifier) {
    val infinite = rememberInfiniteTransition(label = "logo_flicker")
    val alpha by infinite.animateFloat(
        initialValue = 1f,
        targetValue = 1f,
        animationSpec = infiniteRepeatable(
            animation = keyframes {
                durationMillis = 2600
                1f at 0
                0.72f at 70
                1f at 140
                0.86f at 520
                1f at 620
                0.62f at 1540
                1f at 1640
                0.9f at 2140
                1f at 2220
                1f at 2600
            },
            repeatMode = RepeatMode.Restart
        ),
        label = "logo_alpha"
    )

    Image(
        painter = painterResource(id = R.drawable.gitster_logo),
        contentDescription = "GITSTER",
        modifier = modifier
            .padding(horizontal = 10.dp)
            .graphicsLayer(
                alpha = alpha,
                scaleX = 1f - (1f - alpha) * 0.02f,
                scaleY = 1f - (1f - alpha) * 0.02f
            ),
        contentScale = ContentScale.Fit
    )
}

private fun bestSpotifyUriString(res: ScanResolution, card: DeckCard?): String? {
    val candidates = listOfNotNull(
        card?.spotifyUri?.trim()?.takeIf { it.isNotBlank() },
        card?.spotifyUrl?.trim()?.takeIf { it.isNotBlank() },
        res.raw.trim().takeIf { it.startsWith("http", true) || it.startsWith("spotify:", true) },
        (res.parsedTrackId ?: card?.trackId)?.trim()?.takeIf { it.isNotBlank() }?.let { "spotify:track:$it" }
    )

    for (c in candidates) {
        val u = runCatching { Uri.parse(c) }.getOrNull() ?: continue
        if (u.toString().isNotBlank()) return u.toString()
    }
    return null
}
