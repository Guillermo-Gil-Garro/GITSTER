package com.gitster.dj

import android.app.Activity
import android.content.Intent
import android.net.Uri
import android.util.Log
import kotlinx.coroutines.delay

private const val AUTOPLAY_LOG_TAG = "GITSTER_SPOTIFY"

object SpotifyPlaybackController {
    sealed class Result {
        data class Success(val spotifyUri: String, val deviceId: String) : Result()
        data object AuthInProgress : Result()
        data object NoActiveDevice : Result()
        data class Failure(val message: String) : Result()
    }

    suspend fun startAutoplay(
        activity: Activity,
        rawUrl: String?,
        spotifyUri: String?
    ): Result {
        val resolvedUri = resolveFinalTrackUri(rawUrl = rawUrl, spotifyUri = spotifyUri)
        if (resolvedUri.isNullOrBlank()) {
            return Result.Failure("Track URI no valida")
        }

        Log.e(AUTOPLAY_LOG_TAG, "AUTOPLAY start uri=$resolvedUri")
        val token = SpotifyAuthManager.ensureValidToken(activity) ?: return Result.AuthInProgress

        return runWithSingle401Retry(token) { freshToken ->
            autoplayWithToken(activity = activity, token = freshToken, trackUri = resolvedUri)
        }
    }

    private suspend fun autoplayWithToken(
        activity: Activity,
        token: String,
        trackUri: String
    ): Result {
        var devices = SpotifyWebApiClient.getDevices(token)
        Log.e(AUTOPLAY_LOG_TAG, "Devices count=${devices.size}")

        if (devices.isEmpty()) {
            wakeSpotifyApp(activity)
            delay(1_200)
            devices = SpotifyWebApiClient.getDevices(token)
            Log.e(AUTOPLAY_LOG_TAG, "Devices count=${devices.size}")
            if (devices.isEmpty()) {
                Log.e(AUTOPLAY_LOG_TAG, "NoActiveDevice")
                return Result.NoActiveDevice
            }
        }

        val selectedDevice = devices.firstOrNull { it.isActive } ?: devices.first()
        Log.e(AUTOPLAY_LOG_TAG, "Transfer -> deviceId=${selectedDevice.id}")
        SpotifyWebApiClient.transferPlayback(
            token = token,
            deviceId = selectedDevice.id,
            play = true
        )

        SpotifyWebApiClient.playTrack(
            token = token,
            deviceId = selectedDevice.id,
            spotifyTrackUri = trackUri
        )
        Log.e(AUTOPLAY_LOG_TAG, "Play -> ok")
        return Result.Success(spotifyUri = trackUri, deviceId = selectedDevice.id)
    }

    private suspend fun runWithSingle401Retry(
        initialToken: String,
        block: suspend (token: String) -> Result
    ): Result {
        return try {
            block(initialToken)
        } catch (unauthorized: SpotifyWebApiClient.UnauthorizedException) {
            Log.e(AUTOPLAY_LOG_TAG, "AUTOPLAY caught 401, trying refresh+retry once")
            val refreshed = SpotifyAuthManager.refreshIfNeeded()
            val refreshedToken = SpotifyAuthManager.getAccessTokenOrNull()
            if (!refreshed || refreshedToken.isNullOrBlank()) {
                Result.Failure("Spotify auth expirada")
            } else {
                try {
                    block(refreshedToken)
                } catch (second: Throwable) {
                    mapError(second)
                }
            }
        } catch (error: Throwable) {
            mapError(error)
        }
    }

    private fun mapError(error: Throwable): Result {
        return when (error) {
            is SpotifyWebApiClient.NoActiveDeviceException -> {
                Log.e(AUTOPLAY_LOG_TAG, "NoActiveDevice")
                Result.NoActiveDevice
            }

            is SpotifyWebApiClient.UnauthorizedException -> {
                Result.Failure("No autorizado por Spotify")
            }

            else -> {
                val message = error.message?.takeIf { it.isNotBlank() } ?: error.javaClass.simpleName
                Log.e(AUTOPLAY_LOG_TAG, "AUTOPLAY failure: $message", error)
                Result.Failure(message)
            }
        }
    }

    private fun resolveFinalTrackUri(rawUrl: String?, spotifyUri: String?): String? {
        val candidates = listOfNotNull(spotifyUri, rawUrl)
        for (candidate in candidates) {
            val resolved = resolveSpotifyTrackUri(candidate)
            if (!resolved.isNullOrBlank()) {
                return resolved
            }
        }
        return null
    }

    private fun wakeSpotifyApp(activity: Activity) {
        val packageManager = activity.packageManager
        val launchIntent = packageManager.getLaunchIntentForPackage("com.spotify.music")
            ?: packageManager.getLaunchIntentForPackage("com.spotify.lite")
            ?: Intent(Intent.ACTION_VIEW, Uri.parse("spotify:"))

        runCatching {
            launchIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            activity.startActivity(launchIntent)
            Log.e(AUTOPLAY_LOG_TAG, "Wake Spotify app sent")
        }.onFailure { error ->
            Log.e(AUTOPLAY_LOG_TAG, "Wake Spotify failed: ${error.message}", error)
        }
    }
}
