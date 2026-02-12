package com.gitster.dj

import android.net.Uri
import android.util.Log

object SpotifyUriResolver {
    fun resolveSpotifyTrackUri(raw: String): String? {
        val input = raw.trim()
        val result = when {
            input.isBlank() -> null
            input.startsWith("spotify:track:", ignoreCase = true) -> {
                val id = input.substringAfter("spotify:track:", "").substringBefore("?").trim()
                id.takeIf(::isLikelySpotifyTrackId)?.let { "spotify:track:$it" }
            }
            isLikelySpotifyTrackId(input) -> "spotify:track:$input"
            else -> {
                val parsed = runCatching { Uri.parse(input) }.getOrNull()
                val spotifyHost = parsed?.host?.contains("spotify.com", ignoreCase = true) == true
                if (!spotifyHost) {
                    null
                } else {
                    val segments = parsed.pathSegments.orEmpty()
                    val id = if (segments.size >= 2 && segments[0].equals("track", ignoreCase = true)) {
                        segments[1].trim()
                    } else {
                        ""
                    }
                    id.takeIf(::isLikelySpotifyTrackId)?.let { "spotify:track:$it" }
                }
            }
        }

        Log.d(TAG, "resolveSpotifyTrackUri input=$input output=$result")
        return result
    }
}

private fun isLikelySpotifyTrackId(value: String): Boolean {
    return value.length == 22 && value.all { it.isLetterOrDigit() }
}

fun resolveSpotifyTrackUri(raw: String): String? = SpotifyUriResolver.resolveSpotifyTrackUri(raw)
