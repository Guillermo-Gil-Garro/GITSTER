package com.gitster.dj

import android.app.Activity
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.util.Log
import androidx.browser.customtabs.CustomTabsIntent
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.longPreferencesKey
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.FormBody
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject

private const val AUTH_LOG_TAG = "GITSTER_SPOTIFY"
private const val AUTH_STORE_NAME = "spotify_auth"
private const val EXPIRES_BUFFER_MS = 30_000L
private const val AUTH_REOPEN_GUARD_MS = 60_000L

private val Context.spotifyAuthDataStore by preferencesDataStore(name = AUTH_STORE_NAME)

object SpotifyAuthManager {
    private val accessTokenKey = stringPreferencesKey("access_token")
    private val refreshTokenKey = stringPreferencesKey("refresh_token")
    private val expiresAtMsKey = longPreferencesKey("expires_at_ms")
    private val lastStateKey = stringPreferencesKey("last_state")
    private val lastVerifierKey = stringPreferencesKey("last_verifier")

    private val httpClient = OkHttpClient()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val _tokenVersion = MutableStateFlow(0L)
    val tokenVersion: StateFlow<Long> = _tokenVersion.asStateFlow()

    @Volatile
    private var appContext: Context? = null

    @Volatile
    private var authInFlight: Boolean = false

    @Volatile
    private var authStartedAtMs: Long = 0L

    fun initialize(context: Context) {
        appContext = context.applicationContext
    }

    suspend fun ensureValidToken(activity: Activity): String? {
        initialize(activity.applicationContext)
        val context = appContext ?: return null
        val now = System.currentTimeMillis()
        val snapshot = readSnapshot(context)

        if (!snapshot.accessToken.isNullOrBlank() && snapshot.expiresAtMs > now) {
            Log.e(AUTH_LOG_TAG, "OAuth token still valid, expiresAtMs=${snapshot.expiresAtMs}")
            return snapshot.accessToken
        }

        if (refreshIfNeeded()) {
            val refreshed = readSnapshot(context).accessToken
            if (!refreshed.isNullOrBlank()) {
                Log.e(AUTH_LOG_TAG, "OAuth token refreshed and ready")
                return refreshed
            }
        }

        startAuthorization(activity, context)
        return null
    }

    fun handleRedirectIntent(intent: Intent) {
        val data = intent.data ?: return
        if (!isSpotifyCallback(data)) return

        Log.e(AUTH_LOG_TAG, "handleRedirectIntent data=$data")
        val context = appContext
        if (context == null) {
            Log.e(AUTH_LOG_TAG, "handleRedirectIntent ignored: manager not initialized")
            authInFlight = false
            return
        }

        val error = data.getQueryParameter("error")
        if (!error.isNullOrBlank()) {
            Log.e(AUTH_LOG_TAG, "OAuth redirect contains error=$error")
            authInFlight = false
            return
        }

        val code = data.getQueryParameter("code")
        val state = data.getQueryParameter("state")
        if (code.isNullOrBlank() || state.isNullOrBlank()) {
            Log.e(AUTH_LOG_TAG, "OAuth redirect missing code/state")
            authInFlight = false
            return
        }

        scope.launch {
            exchangeAuthorizationCode(context, code, state)
        }
    }

    suspend fun refreshIfNeeded(): Boolean {
        val context = appContext ?: run {
            Log.e(AUTH_LOG_TAG, "refreshIfNeeded skipped: no appContext")
            return false
        }

        val snapshot = readSnapshot(context)
        val now = System.currentTimeMillis()
        if (!snapshot.accessToken.isNullOrBlank() && snapshot.expiresAtMs > now) {
            return true
        }

        val refreshToken = snapshot.refreshToken
        if (refreshToken.isNullOrBlank()) {
            Log.e(AUTH_LOG_TAG, "refreshIfNeeded failed: no refresh token")
            return false
        }

        return refreshAccessToken(context, refreshToken)
    }

    suspend fun getAccessTokenOrNull(): String? {
        val context = appContext ?: return null
        return readSnapshot(context).accessToken
    }

    private fun isSpotifyCallback(uri: Uri): Boolean {
        return uri.scheme.equals("gitster", ignoreCase = true) &&
            uri.host.equals("callback", ignoreCase = true)
    }

    private suspend fun startAuthorization(activity: Activity, context: Context) {
        val now = System.currentTimeMillis()
        if (authInFlight) {
            val elapsed = now - authStartedAtMs
            if (elapsed < AUTH_REOPEN_GUARD_MS) {
                Log.e(AUTH_LOG_TAG, "Auth already in progress; skip reopening Custom Tab")
                return
            }
            Log.e(AUTH_LOG_TAG, "Auth in-flight flag expired after ${elapsed}ms, reopening auth")
            authInFlight = false
        }

        val verifier = SpotifyPkce.generateCodeVerifier()
        val challenge = SpotifyPkce.codeChallengeS256(verifier)
        val state = SpotifyPkce.randomState()

        context.spotifyAuthDataStore.edit { prefs ->
            prefs[lastStateKey] = state
            prefs[lastVerifierKey] = verifier
        }

        val authorizeUri = Uri.parse("https://accounts.spotify.com/authorize")
            .buildUpon()
            .appendQueryParameter("client_id", SpotifyConfig.clientId)
            .appendQueryParameter("response_type", "code")
            .appendQueryParameter("redirect_uri", SpotifyConfig.redirectUri)
            .appendQueryParameter("code_challenge_method", "S256")
            .appendQueryParameter("code_challenge", challenge)
            .appendQueryParameter("state", state)
            .appendQueryParameter(
                "scope",
                "user-modify-playback-state user-read-playback-state"
            )
            .appendQueryParameter("show_dialog", "false")
            .build()

        authInFlight = true
        authStartedAtMs = now
        Log.e(AUTH_LOG_TAG, "Opening Spotify authorize URL=$authorizeUri")

        withContext(Dispatchers.Main) {
            runCatching {
                CustomTabsIntent.Builder().build().launchUrl(activity, authorizeUri)
            }.onFailure { error ->
                authInFlight = false
                Log.e(AUTH_LOG_TAG, "Failed opening auth tab: ${error.message}", error)
            }
        }
    }

    private suspend fun exchangeAuthorizationCode(
        context: Context,
        code: String,
        redirectState: String
    ) {
        Log.e(AUTH_LOG_TAG, "Token exchange start (authorization_code)")
        val snapshot = readSnapshot(context)

        if (snapshot.lastState != redirectState) {
            authInFlight = false
            Log.e(
                AUTH_LOG_TAG,
                "Token exchange aborted: state mismatch expected=${snapshot.lastState} got=$redirectState"
            )
            return
        }

        val verifier = snapshot.lastVerifier
        if (verifier.isNullOrBlank()) {
            authInFlight = false
            Log.e(AUTH_LOG_TAG, "Token exchange aborted: missing PKCE verifier")
            return
        }

        val body = FormBody.Builder()
            .add("grant_type", "authorization_code")
            .add("client_id", SpotifyConfig.clientId)
            .add("code", code)
            .add("redirect_uri", SpotifyConfig.redirectUri)
            .add("code_verifier", verifier)
            .build()

        val request = Request.Builder()
            .url("https://accounts.spotify.com/api/token")
            .post(body)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .build()

        val result = executeTokenRequest(request)
        if (result == null) {
            authInFlight = false
            return
        }

        if (result.code !in 200..299) {
            authInFlight = false
            Log.e(AUTH_LOG_TAG, "Token exchange failed status=${result.code} body=${result.previewBody}")
            return
        }

        runCatching {
            val json = JSONObject(result.rawBody)
            val accessToken = json.optString("access_token", "")
            val refreshToken = json.optString("refresh_token", "")
            val expiresInSec = json.optLong("expires_in", 0L)

            if (accessToken.isBlank() || expiresInSec <= 0L) {
                Log.e(AUTH_LOG_TAG, "Token exchange response missing access_token/expires_in")
                return@runCatching false
            }

            val expiresAtMs = System.currentTimeMillis() + (expiresInSec * 1_000L) - EXPIRES_BUFFER_MS
            context.spotifyAuthDataStore.edit { prefs ->
                prefs[accessTokenKey] = accessToken
                prefs[expiresAtMsKey] = expiresAtMs
                if (refreshToken.isNotBlank()) {
                    prefs[refreshTokenKey] = refreshToken
                }
                prefs.remove(lastStateKey)
                prefs.remove(lastVerifierKey)
            }

            bumpTokenVersion()
            Log.e(
                AUTH_LOG_TAG,
                "Token exchange success token saved expiresAtMs=$expiresAtMs refreshSaved=${refreshToken.isNotBlank()}"
            )
            true
        }.onFailure { error ->
            Log.e(AUTH_LOG_TAG, "Token exchange parse failure: ${error.message}", error)
        }

        authInFlight = false
    }

    private suspend fun refreshAccessToken(context: Context, refreshToken: String): Boolean {
        Log.e(AUTH_LOG_TAG, "Refreshing access token")
        val body = FormBody.Builder()
            .add("grant_type", "refresh_token")
            .add("client_id", SpotifyConfig.clientId)
            .add("refresh_token", refreshToken)
            .build()

        val request = Request.Builder()
            .url("https://accounts.spotify.com/api/token")
            .post(body)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .build()

        val result = executeTokenRequest(request) ?: return false
        if (result.code !in 200..299) {
            Log.e(AUTH_LOG_TAG, "Refresh failed status=${result.code} body=${result.previewBody}")
            return false
        }

        return runCatching {
            val json = JSONObject(result.rawBody)
            val accessToken = json.optString("access_token", "")
            val newRefreshToken = json.optString("refresh_token", "")
            val expiresInSec = json.optLong("expires_in", 0L)
            if (accessToken.isBlank() || expiresInSec <= 0L) {
                Log.e(AUTH_LOG_TAG, "Refresh response missing access_token/expires_in")
                return@runCatching false
            }

            val expiresAtMs = System.currentTimeMillis() + (expiresInSec * 1_000L) - EXPIRES_BUFFER_MS
            context.spotifyAuthDataStore.edit { prefs ->
                prefs[accessTokenKey] = accessToken
                prefs[expiresAtMsKey] = expiresAtMs
                if (newRefreshToken.isNotBlank()) {
                    prefs[refreshTokenKey] = newRefreshToken
                }
            }
            bumpTokenVersion()
            Log.e(
                AUTH_LOG_TAG,
                "Refresh success token saved expiresAtMs=$expiresAtMs refreshRotated=${newRefreshToken.isNotBlank()}"
            )
            true
        }.getOrElse { error ->
            Log.e(AUTH_LOG_TAG, "Refresh parse failure: ${error.message}", error)
            false
        }
    }

    private suspend fun executeTokenRequest(request: Request): TokenResponse? {
        return withContext(Dispatchers.IO) {
            runCatching {
                httpClient.newCall(request).execute().use { response ->
                    val body = response.body?.string().orEmpty()
                    val preview = body.take(500)
                    Log.e(
                        AUTH_LOG_TAG,
                        "Token endpoint status=${response.code} body=$preview"
                    )
                    TokenResponse(
                        code = response.code,
                        rawBody = body,
                        previewBody = preview
                    )
                }
            }.onFailure { error ->
                Log.e(AUTH_LOG_TAG, "Token endpoint call failed: ${error.message}", error)
            }.getOrNull()
        }
    }

    private fun bumpTokenVersion() {
        _tokenVersion.value = _tokenVersion.value + 1L
    }

    private suspend fun readSnapshot(context: Context): TokenSnapshot {
        val prefs = context.spotifyAuthDataStore.data.first()
        return TokenSnapshot(
            accessToken = prefs[accessTokenKey],
            refreshToken = prefs[refreshTokenKey],
            expiresAtMs = prefs[expiresAtMsKey] ?: 0L,
            lastState = prefs[lastStateKey],
            lastVerifier = prefs[lastVerifierKey]
        )
    }

    private data class TokenSnapshot(
        val accessToken: String?,
        val refreshToken: String?,
        val expiresAtMs: Long,
        val lastState: String?,
        val lastVerifier: String?
    )

    private data class TokenResponse(
        val code: Int,
        val rawBody: String,
        val previewBody: String
    )
}
