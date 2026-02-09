package com.gitster.dj

import android.content.Intent
import android.net.Uri
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
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
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

    // Estado UI "tipo Hitster": reproducir automáticamente y cortar a los 60s.
    var isPlaying by rememberSaveable(playable.spotifyUri) { mutableStateOf(true) }
    var remainingSeconds by rememberSaveable(playable.spotifyUri) { mutableStateOf(CAP_SECONDS) }
    var revealed by rememberSaveable(playable.spotifyUri) { mutableStateOf(false) }

    // Autoplay al entrar.
    LaunchedEffect(playable.spotifyUri) {
        isPlaying = true
        revealed = false

        if (!SpotifyConfig.isConfigured()) {
            Toast.makeText(
                context,
                "Spotify aún no configurado (Client ID). De momento es UI dummy.",
                Toast.LENGTH_SHORT
            ).show()
        }
    }

    // Countdown: tiempo efectivo de reproducción (se detiene si pausas)
    LaunchedEffect(isPlaying, remainingSeconds, playable.spotifyUri) {
        if (!isPlaying) return@LaunchedEffect
        while (isPlaying && remainingSeconds > 0) {
            delay(1000)
            remainingSeconds -= 1
        }
        if (remainingSeconds <= 0) {
            isPlaying = false
            Toast.makeText(context, "Tiempo agotado (60s). Pausado.", Toast.LENGTH_SHORT).show()
        }
    }

    val progress = (remainingSeconds.coerceIn(0, CAP_SECONDS)).toFloat() / CAP_SECONDS.toFloat()
    val neonStroke = Brush.linearGradient(listOf(GitsterMagenta, GitsterCyan, GitsterAmber))

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
            // Top bar
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
                        onNextCard()
                    }) { Text("Siguiente", color = GitsterInk, fontWeight = FontWeight.SemiBold) }
                }
            }

            Spacer(Modifier.height(12.dp))

            // Panel central "tipo vídeo"
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .border(2.dp, neonStroke, RoundedCornerShape(22.dp))
                    .background(GitsterPanel, RoundedCornerShape(22.dp))
                    .padding(16.dp)
            ) {
                Text(
                    "Reproducción",
                    color = GitsterInk,
                    fontWeight = FontWeight.Bold
                )
                Spacer(Modifier.height(6.dp))
                Text(
                    if (isPlaying) "▶︎ Sonando… (dummy)" else "⏸ Pausado",
                    color = GitsterMuted
                )

                Spacer(Modifier.height(10.dp))

                // Barra de progreso
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

                // Controles
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.spacedBy(10.dp)
                ) {
                    Button(
                        modifier = Modifier.weight(1f),
                        onClick = {
                            if (remainingSeconds <= 0) return@Button
                            isPlaying = !isPlaying
                        },
                        colors = ButtonDefaults.buttonColors(containerColor = GitsterCyan, contentColor = Color.Black)
                    ) {
                        Text(if (isPlaying) "Pausar" else "Play")
                    }

                    Button(
                        modifier = Modifier.weight(1f),
                        onClick = { revealed = !revealed },
                        colors = ButtonDefaults.buttonColors(containerColor = GitsterMagenta, contentColor = Color.Black)
                    ) {
                        Text(if (revealed) "Ocultar" else "Revelar")
                    }
                }

                if (revealed) {
                    Spacer(Modifier.height(14.dp))
                    HorizontalDivider(color = Color(0x22FFFFFF))
                    Spacer(Modifier.height(12.dp))

                    Text("🎉 Carta", color = GitsterInk, fontWeight = FontWeight.Bold)
                    Spacer(Modifier.height(6.dp))
                    Text(playable.title ?: "(sin título)", color = GitsterInk, fontWeight = FontWeight.SemiBold)
                    Text(playable.artists ?: "(sin artistas)", color = GitsterMuted)
                    Text(
                        playable.year?.let { "Año: $it" } ?: "Año: (desconocido)",
                        color = GitsterMuted
                    )
                }
            }

            Spacer(Modifier.height(12.dp))

            // Debug/estado (MVP OK)
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .background(Color(0x14000000), RoundedCornerShape(18.dp))
                    .padding(12.dp)
            ) {
                Text("Estado del scan", color = GitsterInk, fontWeight = FontWeight.SemiBold)
                Spacer(Modifier.height(6.dp))
                Text("RAW: ${ellipsize(playable.raw)}", color = GitsterMuted)
                Text("kind: ${playable.kind}", color = GitsterMuted)
                Text("card_id: ${playable.cardId ?: "—"}", color = GitsterMuted)
                Text("track_id: ${playable.trackId ?: "—"}", color = GitsterMuted)
            }

            Spacer(Modifier.height(12.dp))

            // Fallback temporal: abrir Spotify (sin App Remote)
            Button(
                modifier = Modifier.fillMaxWidth(),
                onClick = {
                    runCatching {
                        context.startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(playable.spotifyUri)))
                    }.onFailure {
                        Toast.makeText(context, "No puedo abrir Spotify", Toast.LENGTH_SHORT).show()
                    }
                },
                colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF11162E), contentColor = GitsterInk)
            ) {
                Text("Abrir Spotify (fallback)")
            }

            Spacer(Modifier.height(10.dp))
            Text(
                "Próximo paso: integrar Spotify App Remote para reproducir dentro de la app (sin cambiar UX).",
                color = GitsterMuted
            )
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
    return t.take(max - 1) + "…"
}
