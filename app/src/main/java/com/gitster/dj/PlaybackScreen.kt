package com.gitster.dj

import android.content.Intent
import android.net.Uri
import android.util.Log
import android.widget.Toast
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.compose.LocalLifecycleOwner
import com.gitster.dj.ui.theme.GitsterAmber
import com.gitster.dj.ui.theme.GitsterBg0
import com.gitster.dj.ui.theme.GitsterCyan
import com.gitster.dj.ui.theme.GitsterInk
import com.gitster.dj.ui.theme.GitsterMagenta
import com.gitster.dj.ui.theme.GitsterMuted
import com.gitster.dj.ui.theme.GitsterPanel
import kotlinx.coroutines.delay

private const val CAP_SECONDS = 60

@Composable
fun PlaybackScreen(
    playable: PlayableCard,
    onNextCard: () -> Unit,
    onBackHome: () -> Unit
) {
    val context = LocalContext.current
    val activity = context.findActivity()
    val lifecycleOwner = LocalLifecycleOwner.current
    val spotifyManager = remember { SpotifyRemoteManagerHolder.instance }
    val remoteState by spotifyManager.state.collectAsState()
    val authTokenVersion by SpotifyAuthManager.tokenVersion.collectAsState()

    var isPlaying by rememberSaveable(playable.rawUrl) { mutableStateOf(false) }
    var remainingSeconds by rememberSaveable(playable.rawUrl) { mutableStateOf(CAP_SECONDS) }
    var revealed by rememberSaveable(playable.rawUrl) { mutableStateOf(false) }
    var nowPlayingLabel by rememberSaveable(playable.rawUrl) { mutableStateOf("Reproduciendo...") }
    var autoplayStatusText by rememberSaveable(playable.rawUrl) { mutableStateOf("Conectando a Spotify...") }
    var autoplayRunning by rememberSaveable(playable.rawUrl) { mutableStateOf(false) }
    var authInProgress by rememberSaveable(playable.rawUrl) { mutableStateOf(false) }
    var autoplayFailedFinal by rememberSaveable(playable.rawUrl) { mutableStateOf(false) }

    val rawInputOrUrl = playable.rawUrl
    val resolvedSpotifyUri = remember(rawInputOrUrl) {
        SpotifyUriResolver.resolveSpotifyTrackUri(rawInputOrUrl)
    }

    val invalidSpotifyUri = resolvedSpotifyUri.isNullOrBlank()
    val showFallbackButton = !autoplayRunning && !authInProgress && (autoplayFailedFinal || invalidSpotifyUri)

    LaunchedEffect(Unit) {
        Log.e("GITSTER_SPOTIFY", "SMOKE ENTER PlaybackScreen")
    }
    LaunchedEffect(activity, context) {
        Log.e(
            "GITSTER_SPOTIFY",
            "SMOKE activity=" + (activity?.javaClass?.name ?: "null") + " context=" + context.javaClass.name
        )
    }
    LaunchedEffect(rawInputOrUrl, resolvedSpotifyUri) {
        Log.e("GITSTER_SPOTIFY", "SMOKE Playback input=$rawInputOrUrl resolved=$resolvedSpotifyUri")
    }

    LaunchedEffect(playable.rawUrl) {
        isPlaying = false
        revealed = false
        remainingSeconds = CAP_SECONDS
        nowPlayingLabel = "Reproduciendo..."
        autoplayStatusText = "Conectando a Spotify..."
        autoplayRunning = false
        authInProgress = false
        autoplayFailedFinal = false
        if (invalidSpotifyUri) {
            Log.e(TAG, "Invalid spotifyUri from rawUrl=${playable.rawUrl} spotifyUri=${playable.spotifyUri}")
            autoplayStatusText = "No se pudo resolver el track de Spotify."
            autoplayFailedFinal = true
        }
        if (activity == null) {
            Log.e(TAG, "No Activity; cannot auth/connect")
            autoplayStatusText = "No se pudo obtener Activity para Spotify."
            autoplayFailedFinal = true
        }
    }

    LaunchedEffect(activity, resolvedSpotifyUri, authTokenVersion, playable.rawUrl) {
        if (activity == null || resolvedSpotifyUri.isNullOrBlank()) {
            return@LaunchedEffect
        }

        autoplayRunning = true
        authInProgress = false
        autoplayFailedFinal = false
        isPlaying = false
        autoplayStatusText = "Conectando a Spotify..."
        Log.e("GITSTER_SPOTIFY", "AUTOPLAY UI start uri=$resolvedSpotifyUri tokenVersion=$authTokenVersion")

        autoplayStatusText = "Iniciando reproduccion..."
        when (
            val autoplayResult = SpotifyPlaybackController.startAutoplay(
                activity = activity,
                rawUrl = rawInputOrUrl,
                spotifyUri = resolvedSpotifyUri
            )
        ) {
            is SpotifyPlaybackController.Result.Success -> {
                autoplayRunning = false
                authInProgress = false
                autoplayFailedFinal = false
                isPlaying = true
                nowPlayingLabel = "Reproduciendo..."
                autoplayStatusText = "Reproduciendo"
            }

            SpotifyPlaybackController.Result.AuthInProgress -> {
                autoplayRunning = false
                authInProgress = true
                autoplayFailedFinal = false
                isPlaying = false
                autoplayStatusText = "Inicia sesion en Spotify..."
            }

            SpotifyPlaybackController.Result.NoActiveDevice -> {
                autoplayRunning = false
                authInProgress = false
                autoplayFailedFinal = true
                isPlaying = false
                autoplayStatusText = "No hay dispositivo activo de Spotify."
            }

            is SpotifyPlaybackController.Result.Failure -> {
                autoplayRunning = false
                authInProgress = false
                autoplayFailedFinal = true
                isPlaying = false
                autoplayStatusText = "Error de autoplay: ${autoplayResult.message}"
            }
        }
    }

    LaunchedEffect(activity, resolvedSpotifyUri, autoplayFailedFinal) {
        if (!autoplayFailedFinal || activity == null || resolvedSpotifyUri.isNullOrBlank()) {
            return@LaunchedEffect
        }
        if (remoteState == RemoteState.CONNECTED || remoteState == RemoteState.CONNECTING) {
            return@LaunchedEffect
        }
        Log.e("GITSTER_SPOTIFY", "AUTOPLAY failed, trying App Remote best-effort")
        spotifyManager.connect(
            activity = activity,
            spotifyUri = resolvedSpotifyUri,
            onPlayerState = { state ->
                isPlaying = !state.isPaused
                val track = state.track?.name?.trim().orEmpty()
                val artist = state.track?.artist?.name?.trim().orEmpty()
                nowPlayingLabel = when {
                    track.isBlank() && artist.isBlank() -> "Reproduciendo..."
                    artist.isBlank() -> track
                    else -> "$track - $artist"
                }
            }
        )
    }

    DisposableEffect(lifecycleOwner) {
        val observer = LifecycleEventObserver { _, event ->
            when (event) {
                Lifecycle.Event.ON_STOP -> {
                    spotifyManager.disconnect("PlaybackScreen.ON_STOP")
                }
                else -> Unit
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose {
            lifecycleOwner.lifecycle.removeObserver(observer)
        }
    }

    LaunchedEffect(isPlaying, remainingSeconds, playable.rawUrl) {
        if (!isPlaying) return@LaunchedEffect
        while (isPlaying && remainingSeconds > 0) {
            delay(1000)
            remainingSeconds -= 1
        }
        if (remainingSeconds <= 0) {
            isPlaying = false
            spotifyManager.spotifyAppRemote?.playerApi?.pause()
            Toast.makeText(context, "Tiempo agotado (60s). Pausado.", Toast.LENGTH_SHORT).show()
        }
    }

    val progress = (remainingSeconds.coerceIn(0, CAP_SECONDS)).toFloat() / CAP_SECONDS.toFloat()
    val neonStroke = Brush.linearGradient(listOf(GitsterMagenta, GitsterCyan, GitsterAmber))

    fun openTrackInSpotify(rawUrl: String?, spotifyUri: String?) {
        val ctx = context
        val spotifyPkg = "com.spotify.music"

        val url = rawUrl ?: spotifyUri?.let { "https://open.spotify.com/track/" + it.substringAfterLast(":") }
        if (url == null) return

        val i1 = Intent(Intent.ACTION_VIEW, Uri.parse(url)).apply {
            setPackage(spotifyPkg)
            addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        }

        val i2 = spotifyUri?.let {
            Intent(Intent.ACTION_VIEW, Uri.parse(it)).apply {
                setPackage(spotifyPkg)
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            }
        }

        try {
            Log.e("GITSTER_SPOTIFY", "Fallback openTrack i1 url=$url")
            ctx.startActivity(i1)
        } catch (_: Throwable) {
            try {
                if (i2 != null) {
                    Log.e("GITSTER_SPOTIFY", "Fallback openTrack i2 uri=$spotifyUri")
                    ctx.startActivity(i2)
                } else {
                    Log.e("GITSTER_SPOTIFY", "Fallback openTrack web url=$url")
                    ctx.startActivity(
                        Intent(Intent.ACTION_VIEW, Uri.parse(url)).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                    )
                }
            } catch (t2: Throwable) {
                Log.e("GITSTER_SPOTIFY", "Fallback openTrack FAILED", t2)
                Toast.makeText(context, "No puedo abrir Spotify", Toast.LENGTH_SHORT).show()
            }
        }
    }

    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(
                Brush.verticalGradient(
                    listOf(
                        GitsterBg0,
                        Color(0xFF070B1C),
                        Color(0xFF060814)
                    )
                )
            )
            .padding(14.dp)
    ) {
        Column(
            modifier = Modifier.fillMaxSize(),
            verticalArrangement = Arrangement.Top
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    "GITSTER",
                    color = GitsterInk,
                    fontWeight = FontWeight.Black
                )
                Row(verticalAlignment = Alignment.CenterVertically) {
                    TextButton(onClick = onBackHome) { Text("Home", color = GitsterInk) }
                    Spacer(Modifier.width(6.dp))
                    TextButton(onClick = {
                        isPlaying = false
                        spotifyManager.spotifyAppRemote?.playerApi?.pause()
                        onNextCard()
                    }) { Text("Siguiente", color = GitsterInk, fontWeight = FontWeight.SemiBold) }
                }
            }

            Spacer(Modifier.height(12.dp))

            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .border(2.dp, neonStroke, RoundedCornerShape(22.dp))
                    .background(GitsterPanel, RoundedCornerShape(22.dp))
                    .padding(16.dp)
            ) {
                Text(
                    "Reproduccion",
                    color = GitsterInk,
                    fontWeight = FontWeight.Bold
                )
                Spacer(Modifier.height(6.dp))
                val statusText = when {
                    autoplayRunning -> autoplayStatusText
                    authInProgress -> "Inicia sesion en Spotify..."
                    autoplayFailedFinal -> autoplayStatusText
                    remoteState == RemoteState.CONNECTING -> "Conectando a Spotify..."
                    isPlaying -> "Reproduciendo: $nowPlayingLabel"
                    else -> "Pausado: $nowPlayingLabel"
                }
                Text(statusText, color = GitsterMuted)

                Spacer(Modifier.height(10.dp))

                Box(
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(10.dp)
                        .background(Color(0x22000000), RoundedCornerShape(99.dp))
                ) {
                    Box(
                        modifier = Modifier
                            .fillMaxWidth(progress)
                            .height(10.dp)
                            .background(GitsterCyan, RoundedCornerShape(99.dp))
                    )
                }
                Spacer(Modifier.height(8.dp))
                Text(
                    "Tiempo restante: ${formatMmSs(remainingSeconds)}",
                    color = GitsterInk,
                    fontWeight = FontWeight.SemiBold
                )

                Spacer(Modifier.height(14.dp))

                if (remoteState == RemoteState.CONNECTED) {
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(10.dp)
                    ) {
                        Button(
                            modifier = Modifier.weight(1f),
                            onClick = {
                            if (remainingSeconds <= 0) return@Button
                            val remote = spotifyManager.spotifyAppRemote
                            val uri = resolvedSpotifyUri
                                if (remote == null || uri == null) {
                                    Toast.makeText(context, "Spotify no disponible.", Toast.LENGTH_SHORT).show()
                                    return@Button
                                }
                                if (isPlaying) {
                                    Log.d(TAG, "pause() called")
                                    remote.playerApi.pause()
                                        .setErrorCallback { throwable ->
                                            Log.e(TAG, "pause() error: ${throwable.message}", throwable)
                                            Toast.makeText(context, "No se pudo pausar.", Toast.LENGTH_SHORT).show()
                                        }
                                } else {
                                    Log.d(TAG, "resume() called")
                                    remote.playerApi.resume()
                                        .setErrorCallback { throwable ->
                                            Log.e(TAG, "resume() error: ${throwable.message}", throwable)
                                            Log.d(TAG, "play(uri) retry called uri=$uri")
                                            remote.playerApi.play(uri)
                                                .setErrorCallback { playErr ->
                                                    Log.e(TAG, "play(uri) retry error: ${playErr.message}", playErr)
                                                    Toast.makeText(context, "No se pudo reanudar.", Toast.LENGTH_SHORT).show()
                                                }
                                        }
                                }
                            },
                            colors = ButtonDefaults.buttonColors(containerColor = GitsterCyan, contentColor = Color.Black)
                        ) {
                            Text(if (isPlaying) "Pausar" else "Reanudar")
                        }

                        Button(
                            modifier = Modifier.weight(1f),
                            onClick = { revealed = !revealed },
                            colors = ButtonDefaults.buttonColors(containerColor = GitsterMagenta, contentColor = Color.Black)
                        ) {
                            Text(if (revealed) "Ocultar" else "Revelar")
                        }
                    }
                }

                if (revealed) {
                    Spacer(Modifier.height(14.dp))
                    HorizontalDivider(color = Color(0x22FFFFFF))
                    Spacer(Modifier.height(12.dp))

                    Text("Carta", color = GitsterInk, fontWeight = FontWeight.Bold)
                    Spacer(Modifier.height(6.dp))
                    Text(playable.title ?: "(sin titulo)", color = GitsterInk, fontWeight = FontWeight.SemiBold)
                    Text(playable.artists ?: "(sin artistas)", color = GitsterMuted)
                    Text(
                        playable.year?.let { "Ano: $it" } ?: "Ano: (desconocido)",
                        color = GitsterMuted
                    )
                }
            }

            Spacer(Modifier.height(12.dp))

            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .background(Color(0x14000000), RoundedCornerShape(18.dp))
                    .padding(12.dp)
            ) {
                Text("Estado del scan", color = GitsterInk, fontWeight = FontWeight.SemiBold)
                Spacer(Modifier.height(6.dp))
                Text("RAW: ${ellipsize(playable.rawUrl)}", color = GitsterMuted)
                Text("kind: ${playable.kind}", color = GitsterMuted)
                Text("card_id: ${playable.cardId ?: "-"}", color = GitsterMuted)
                Text("track_id: ${playable.trackId ?: "-"}", color = GitsterMuted)
            }

            if (showFallbackButton) {
                Spacer(Modifier.height(12.dp))
                Button(
                    modifier = Modifier.fillMaxWidth(),
                    onClick = {
                        Log.e(TAG, "UI click: fallback openTrackInSpotify")
                        openTrackInSpotify(playable.rawUrl, resolvedSpotifyUri)
                    },
                    colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF11162E), contentColor = GitsterInk)
                ) {
                    Text("Abrir Spotify")
                }
            }
        }
    }
}

private fun formatMmSs(seconds: Int): String {
    val s = seconds.coerceAtLeast(0)
    val mm = s / 60
    val ss = s % 60
    return "%d:%02d".format(mm, ss)
}

private fun ellipsize(s: String, max: Int = 48): String {
    val t = s.trim()
    if (t.length <= max) return t
    return t.take(max - 1) + "..."
}
