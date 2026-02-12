package com.gitster.dj

import android.app.Activity
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import android.os.SystemClock
import android.util.Log
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import com.spotify.android.appremote.api.ConnectionParams
import com.spotify.android.appremote.api.Connector
import com.spotify.android.appremote.api.SpotifyAppRemote
import com.spotify.protocol.client.Subscription
import com.spotify.protocol.types.PlayerState
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch

const val TAG = "GITSTER_SPOTIFY"
const val CLIENT_ID = "7ea7ec0d13a24452ab34044dbc976bd9"
const val REDIRECT_URI = "gitster://callback"

enum class RemoteState { IDLE, CONNECTING, CONNECTED, FAILED, HUNG }

private const val SPOTIFY_PACKAGE_NAME = "com.spotify.music"
private const val MAX_CONNECT_ATTEMPTS = 2

class SpotifyRemoteManager {
    val state = MutableStateFlow(RemoteState.IDLE)
    var lastError: String? by mutableStateOf(null)
    var spotifyAppRemote: SpotifyAppRemote? by mutableStateOf(null)
        private set

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private var watchdogJob: Job? = null
    var pendingUri: String? = null
    var retryOnNextResume: Boolean = false
    var permanentFallback: Boolean by mutableStateOf(false)
    private var playerStateSubscription: Subscription<PlayerState>? = null
    private var connectStartedAtMs: Long = 0L
    private var attemptCount: Int = 0

    fun isPackageInstalled(context: Context, packageName: String): Boolean {
        return try {
            val pm = context.packageManager
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                pm.getPackageInfo(packageName, PackageManager.PackageInfoFlags.of(0))
            } else {
                @Suppress("DEPRECATION")
                pm.getPackageInfo(packageName, 0)
            }
            true
        } catch (_: Throwable) {
            false
        }
    }

    fun connect(
        activity: Activity,
        spotifyUri: String,
        onPlayerState: (PlayerState) -> Unit = {}
    ) {
        if (pendingUri != spotifyUri) {
            attemptCount = 0
            permanentFallback = false
            pendingUri = spotifyUri
            Log.e(TAG, "New track uri detected, reset attempts/permanentFallback")
        }

        if (permanentFallback) {
            Log.e(TAG, "connect skipped: permanentFallback=true")
            return
        }

        if (state.value == RemoteState.CONNECTING) {
            Log.e(TAG, "CONNECT skipped: already CONNECTING")
            return
        }

        if (attemptCount >= MAX_CONNECT_ATTEMPTS) {
            permanentFallback = true
            state.value = RemoteState.FAILED
            lastError = "AppRemote hung repeatedly"
            Log.e(TAG, "connect blocked: attempts exhausted -> permanentFallback=true")
            return
        }

        attemptCount += 1
        connectStartedAtMs = SystemClock.uptimeMillis()
        Log.e(TAG, "CONNECT attempt=$attemptCount start uri=$spotifyUri at=$connectStartedAtMs")
        pendingUri = spotifyUri
        lastError = null
        state.value = RemoteState.CONNECTING

        watchdogJob?.cancel()
        watchdogJob = scope.launch {
            delay(2500)
            if (state.value == RemoteState.CONNECTING) {
                Log.e(TAG, "CONNECT HANG detected attempt=$attemptCount (no callbacks)")
                state.value = RemoteState.HUNG
                if (attemptCount >= MAX_CONNECT_ATTEMPTS) {
                    permanentFallback = true
                    Log.e(TAG, "permanentFallback=true (after repeated hangs)")
                }
            }
        }

        val params = ConnectionParams.Builder(CLIENT_ID)
            .setRedirectUri(REDIRECT_URI)
            .showAuthView(true)
            .build()

        activity.runOnUiThread {
            Log.e(TAG, "SMOKE ENTER connect() uri=$spotifyUri")
            SpotifyAppRemote.connect(
                activity,
                params,
                object : Connector.ConnectionListener {
                    override fun onConnected(appRemote: SpotifyAppRemote) {
                        watchdogJob?.cancel()
                        spotifyAppRemote = appRemote
                        state.value = RemoteState.CONNECTED
                        permanentFallback = false
                        Log.e(TAG, "onConnected()")

                        playerStateSubscription?.cancel()
                        val subscription = appRemote.playerApi.subscribeToPlayerState()
                        subscription.setEventCallback { ps ->
                            Log.e(TAG, "playerState trackUri=${ps.track?.uri} isPaused=${ps.isPaused}")
                            onPlayerState(ps)
                        }
                        subscription.setErrorCallback { t ->
                            Log.e(TAG, "subscribeToPlayerState error: ${t.message}", t)
                        }
                        playerStateSubscription = subscription

                        val uri = pendingUri
                        if (!uri.isNullOrBlank()) {
                            Log.e(TAG, "play(uri) uri=$uri")
                            appRemote.playerApi.play(uri)
                                .setErrorCallback { t ->
                                    Log.e(TAG, "play(uri) error: ${t.message}", t)
                                }
                        }
                    }

                    override fun onFailure(t: Throwable) {
                        watchdogJob?.cancel()
                        spotifyAppRemote = null
                        lastError = "${t.javaClass.simpleName}: ${t.message}"
                        state.value = RemoteState.FAILED
                        Log.e(TAG, "onFailure ${t.javaClass.name}: ${t.message}", t)
                    }
                }
            )
        }
    }

    fun disconnect(reason: String = "unknown") {
        val now = SystemClock.uptimeMillis()
        Log.e(
            TAG,
            "disconnect(reason=$reason) state=${state.value} dt=${now - connectStartedAtMs}ms",
            Exception("disconnect stack")
        )

        if (state.value == RemoteState.CONNECTING &&
            reason.contains("ON_STOP") &&
            (now - connectStartedAtMs) < 5000
        ) {
            Log.e(TAG, "IGNORE disconnect during CONNECTING (likely auth/overlay).")
            return
        }

        watchdogJob?.cancel()
        watchdogJob = null
        playerStateSubscription?.cancel()
        playerStateSubscription = null
        spotifyAppRemote?.let { SpotifyAppRemote.disconnect(it) }
        spotifyAppRemote = null

        if (reason.contains("ON_STOP") &&
            (state.value == RemoteState.HUNG || state.value == RemoteState.FAILED)
        ) {
            Log.e(TAG, "Keep state=${state.value} across ON_STOP to allow retry UI")
            return
        }

        state.value = RemoteState.IDLE
    }

    fun requestRetryOnResume(reason: String = "manual") {
        retryOnNextResume = true
        Log.e(TAG, "requestRetryOnResume reason=$reason pendingUri=$pendingUri state=${state.value}")
    }

    fun retryIfNeeded(activity: Activity) {
        if (!retryOnNextResume) return
        val uri = pendingUri
        Log.e(TAG, "retryIfNeeded retryOnNextResume=true state=${state.value} uri=$uri")
        retryOnNextResume = false
        if (!uri.isNullOrBlank() &&
            (state.value == RemoteState.HUNG || state.value == RemoteState.FAILED || state.value == RemoteState.IDLE)
        ) {
            scope.launch {
                delay(800)
                connect(activity, uri)
                Log.e(TAG, "Retry connect executed")
            }
        }
    }
}

object SpotifyRemoteManagerHolder {
    val instance: SpotifyRemoteManager = SpotifyRemoteManager()
}
