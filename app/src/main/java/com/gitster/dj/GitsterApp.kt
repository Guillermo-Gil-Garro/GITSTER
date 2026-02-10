package com.gitster.dj

import android.content.Intent
import android.net.Uri
import android.widget.Toast
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.keyframes
import androidx.compose.animation.core.tween
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
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
import androidx.compose.ui.draw.blur
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Shadow
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.graphics.drawscope.rotate
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
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
                        0f to Color(0x66060814),
                        0.55f to Color(0x77060814),
                        1f to Color(0x99060814)
                    )
                )
        )

        Column(
            modifier = Modifier
                .fillMaxSize()
                .safeDrawingPadding()
                .padding(horizontal = 18.dp, vertical = 16.dp),
            verticalArrangement = Arrangement.Top,
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Column(
                Modifier.fillMaxWidth().weight(1f),
                horizontalAlignment = Alignment.CenterHorizontally
            ) {
                Spacer(Modifier.height(2.dp))
                NeonFlickerLogo(modifier = Modifier.fillMaxWidth())
                Box(
                    modifier = Modifier.fillMaxWidth().weight(1f),
                    contentAlignment = Alignment.Center
                ) {
                    HomeQrHero()
                }

                Spacer(Modifier.height(8.dp))
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

                Button(
                    onClick = onRules,
                    modifier = Modifier.fillMaxWidth().height(58.dp),
                    shape = shapeLg,
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.primary,
                        contentColor = Color.Black
                    )
                ) {
                    Text(
                        "REGLAS",
                        fontWeight = FontWeight.Black,
                        style = MaterialTheme.typography.titleMedium
                    )
                }
            }
        }
    }
}

@Composable
private fun HomeQrHero() {
    val haptic = LocalHapticFeedback.current
    val qrSize = 150.dp
    val cardPadding = 12.dp
    val frameSize = 240.dp
    val frameGlowPadding = 22.dp

    Column(
        horizontalAlignment = Alignment.CenterHorizontally
    ) {
        Box(
            modifier = Modifier
                .size(frameSize + frameGlowPadding * 2),
            contentAlignment = Alignment.Center
        ) {
            NeonQrFrame(
                modifier = Modifier
                    .matchParentSize()
                    .padding(frameGlowPadding)
            )

            Box(
                modifier = Modifier
                    .size(qrSize + cardPadding * 2)
                    .clip(RoundedCornerShape(14.dp))
                    .background(Color(0xFFFDFDFD))
                    .padding(cardPadding)
                    .clickable {
                        haptic.performHapticFeedback(HapticFeedbackType.TextHandleMove)
                    },
                contentAlignment = Alignment.Center
            ) {
                Image(
                    painter = painterResource(id = R.drawable.qr_home),
                    contentDescription = "QR Home",
                    modifier = Modifier.size(qrSize),
                    contentScale = ContentScale.Fit
                )
            }
        }

        Spacer(Modifier.height(10.dp))
        ScanMeNeonText()
    }
}

@Composable
private fun NeonQrFrame(modifier: Modifier = Modifier) {
    val infinite = rememberInfiniteTransition(label = "qr_neon_ring")
    val rotation by infinite.animateFloat(
        initialValue = 0f,
        targetValue = 360f,
        animationSpec = infiniteRepeatable(
            animation = tween(durationMillis = 7000),
            repeatMode = RepeatMode.Restart
        ),
        label = "qr_ring_rotation"
    )
    val pulse by infinite.animateFloat(
        initialValue = 0.98f,
        targetValue = 1.03f,
        animationSpec = infiniteRepeatable(
            animation = tween(durationMillis = 3000),
            repeatMode = RepeatMode.Reverse
        ),
        label = "qr_ring_pulse"
    )

    val colorMagenta = Color(0xFFFF2FD0)
    val neonColors = listOf(
        colorMagenta,
        Color(0xFFFF63C9),
        Color(0xFF9D4DFF),
        Color(0xFF4B7BFF),
        Color(0xFF26D7FF),
        Color(0xFF4FFFD2),
        Color(0xFFFFE45E),
        Color(0xFFFF9E2C),
        colorMagenta
    )

    Canvas(
        modifier = modifier.graphicsLayer(
            scaleX = pulse,
            scaleY = pulse,
            alpha = 0.95f
        )
    ) {
        val strokeOuter = size.minDimension * 0.13f
        val strokeMid = size.minDimension * 0.085f
        val strokeInner = size.minDimension * 0.045f
        val maxStrokeWidthPx = maxOf(strokeOuter, strokeMid, strokeInner)
        val extraPadPx = 8.dp.toPx()
        val insetPx = (maxStrokeWidthPx * 0.5f) + extraPadPx
        val frameRect = Rect(
            left = insetPx,
            top = insetPx,
            right = size.width - insetPx,
            bottom = size.height - insetPx
        )
        val cornerOuter = 26.dp.toPx()
        val cornerMid = 24.dp.toPx()
        val cornerInner = 22.dp.toPx()
        val sweep = Brush.sweepGradient(neonColors)

        rotate(rotation) {
            drawRoundRect(
                brush = sweep,
                topLeft = frameRect.topLeft,
                size = frameRect.size,
                cornerRadius = androidx.compose.ui.geometry.CornerRadius(cornerOuter, cornerOuter),
                style = Stroke(width = strokeOuter),
                alpha = 0.20f
            )
            drawRoundRect(
                brush = sweep,
                topLeft = frameRect.topLeft,
                size = frameRect.size,
                cornerRadius = androidx.compose.ui.geometry.CornerRadius(cornerMid, cornerMid),
                style = Stroke(width = strokeMid),
                alpha = 0.42f
            )
            drawRoundRect(
                brush = sweep,
                topLeft = frameRect.topLeft,
                size = frameRect.size,
                cornerRadius = androidx.compose.ui.geometry.CornerRadius(cornerInner, cornerInner),
                style = Stroke(width = strokeInner),
                alpha = 0.98f
            )
        }
    }
}

@Composable
private fun ScanMeNeonText() {
    val scanMeText = "SCAN ME!"
    val infinite = rememberInfiniteTransition(label = "scan_me_flicker")
    val flickerAlpha by infinite.animateFloat(
        initialValue = 1f,
        targetValue = 1f,
        animationSpec = infiniteRepeatable(
            animation = keyframes {
                durationMillis = 1500
                1f at 0
                0.84f at 190
                1f at 280
                0.72f at 780
                1f at 890
                0.88f at 1220
                1f at 1500
            },
            repeatMode = RepeatMode.Restart
        ),
        label = "scan_me_alpha"
    )

    Box(
        modifier = Modifier.offset(y = (-10).dp),
        contentAlignment = Alignment.Center
    ) {
        Text(
            text = scanMeText,
            color = Color(0xFFD7FF4A).copy(alpha = 0.42f * flickerAlpha),
            style = MaterialTheme.typography.titleLarge.copy(
                fontWeight = FontWeight.ExtraBold,
                shadow = Shadow(
                    color = Color.Black.copy(alpha = 0.82f),
                    blurRadius = 28f
                )
            ),
            modifier = Modifier
                .graphicsLayer(
                    scaleX = 1.04f,
                    scaleY = 1.04f
                )
                .blur(7.dp)
        )
        Text(
            text = scanMeText,
            color = Color(0xFFD7FF4A).copy(alpha = 0.32f * flickerAlpha),
            style = MaterialTheme.typography.titleLarge.copy(
                fontWeight = FontWeight.ExtraBold,
                shadow = Shadow(
                    color = Color(0xFFD7FF4A).copy(alpha = 0.9f),
                    blurRadius = 24f
                )
            )
        )
        Text(
            text = scanMeText,
            color = Color.White.copy(alpha = 0.96f * flickerAlpha),
            style = MaterialTheme.typography.titleLarge.copy(
                fontWeight = FontWeight.ExtraBold,
                shadow = Shadow(
                    color = Color(0xFFD7FF4A).copy(alpha = 0.68f),
                    blurRadius = 10f
                )
            )
        )
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
                durationMillis = 9000
                1f at 0
                0.86f at 1800
                1f at 1940
                0.74f at 5300
                1f at 5420
                0.82f at 5560
                1f at 5700
                1f at 9000
            },
            repeatMode = RepeatMode.Restart
        ),
        label = "logo_alpha"
    )

    Box(modifier = modifier, contentAlignment = Alignment.Center) {
        Image(
            painter = painterResource(id = R.drawable.gitster_logo),
            contentDescription = null,
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 10.dp)
                .graphicsLayer(
                    alpha = 0.26f * alpha,
                    scaleX = 1.03f,
                    scaleY = 1.03f
                )
                .blur(12.dp),
            contentScale = ContentScale.Fit
        )

        Image(
            painter = painterResource(id = R.drawable.gitster_logo),
            contentDescription = "GITSTER",
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 10.dp)
                .graphicsLayer(
                    alpha = alpha,
                    scaleX = 1f - (1f - alpha) * 0.01f,
                    scaleY = 1f - (1f - alpha) * 0.01f
                ),
            contentScale = ContentScale.Fit
        )
    }
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
