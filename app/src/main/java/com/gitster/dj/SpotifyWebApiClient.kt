package com.gitster.dj

import android.net.Uri
import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException

private const val WEB_API_LOG_TAG = "GITSTER_SPOTIFY"

object SpotifyWebApiClient {
    private val client = OkHttpClient()
    private val jsonMediaType = "application/json".toMediaType()

    data class Device(
        val id: String,
        val name: String,
        val isActive: Boolean
    )

    open class WebApiException(
        message: String,
        val statusCode: Int,
        val bodyPreview: String
    ) : IOException(message)

    class UnauthorizedException(
        statusCode: Int,
        bodyPreview: String
    ) : WebApiException("Unauthorized from Spotify Web API", statusCode, bodyPreview)

    class NoActiveDeviceException(
        statusCode: Int,
        bodyPreview: String
    ) : WebApiException("No active Spotify device", statusCode, bodyPreview)

    class UnexpectedStatusException(
        statusCode: Int,
        bodyPreview: String,
        endpoint: String
    ) : WebApiException("Unexpected status from $endpoint", statusCode, bodyPreview)

    fun authorizedRequest(
        token: String,
        method: String,
        url: String,
        bodyJson: String?
    ): Response {
        val requestBuilder = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $token")

        val upperMethod = method.uppercase()
        when {
            bodyJson != null -> {
                requestBuilder.method(upperMethod, bodyJson.toRequestBody(jsonMediaType))
            }

            upperMethod == "PUT" || upperMethod == "POST" || upperMethod == "PATCH" -> {
                requestBuilder.method(upperMethod, "".toRequestBody(jsonMediaType))
            }

            else -> {
                requestBuilder.method(upperMethod, null)
            }
        }

        return client.newCall(requestBuilder.build()).execute()
    }

    suspend fun getDevices(token: String): List<Device> {
        val endpoint = "GET /v1/me/player/devices"
        val result = executeWithRefreshRetry(
            initialToken = token,
            method = "GET",
            url = "https://api.spotify.com/v1/me/player/devices",
            bodyJson = null
        )

        if (result.statusCode in 200..299) {
            val json = JSONObject(result.body)
            val devices = json.optJSONArray("devices") ?: JSONArray()
            return buildList {
                for (i in 0 until devices.length()) {
                    val item = devices.optJSONObject(i) ?: continue
                    val id = item.optString("id", "")
                    if (id.isBlank()) continue
                    add(
                        Device(
                            id = id,
                            name = item.optString("name", "Spotify device"),
                            isActive = item.optBoolean("is_active", false)
                        )
                    )
                }
            }
        }

        throwStatus(endpoint = endpoint, result = result)
    }

    suspend fun transferPlayback(token: String, deviceId: String, play: Boolean) {
        val endpoint = "PUT /v1/me/player"
        val body = JSONObject()
            .put("device_ids", JSONArray().put(deviceId))
            .put("play", play)
            .toString()

        val result = executeWithRefreshRetry(
            initialToken = token,
            method = "PUT",
            url = "https://api.spotify.com/v1/me/player",
            bodyJson = body
        )

        if (result.statusCode in 200..299) return
        throwStatus(endpoint = endpoint, result = result)
    }

    suspend fun playTrack(token: String, deviceId: String?, spotifyTrackUri: String) {
        val endpoint = "PUT /v1/me/player/play"
        val playUri = Uri.parse("https://api.spotify.com/v1/me/player/play")
            .buildUpon()
            .apply {
                if (!deviceId.isNullOrBlank()) appendQueryParameter("device_id", deviceId)
            }
            .build()
            .toString()

        val body = JSONObject()
            .put("uris", JSONArray().put(spotifyTrackUri))
            .toString()

        val result = executeWithRefreshRetry(
            initialToken = token,
            method = "PUT",
            url = playUri,
            bodyJson = body
        )

        if (result.statusCode in 200..299) return
        throwStatus(endpoint = endpoint, result = result)
    }

    private suspend fun executeWithRefreshRetry(
        initialToken: String,
        method: String,
        url: String,
        bodyJson: String?
    ): HttpResult {
        var token = initialToken
        var retried = false
        while (true) {
            val result = withContext(Dispatchers.IO) { executeOnce(token, method, url, bodyJson) }
            if (result.statusCode != 401) {
                return result
            }

            if (retried) {
                return result
            }

            Log.e(WEB_API_LOG_TAG, "Web API 401 -> trying token refresh and retry")
            val refreshed = SpotifyAuthManager.refreshIfNeeded()
            val newToken = SpotifyAuthManager.getAccessTokenOrNull()
            if (!refreshed || newToken.isNullOrBlank()) {
                return result
            }
            token = newToken
            retried = true
        }
    }

    private fun executeOnce(
        token: String,
        method: String,
        url: String,
        bodyJson: String?
    ): HttpResult {
        return authorizedRequest(token, method, url, bodyJson).use { response ->
            val body = response.body?.string().orEmpty()
            val preview = body.take(500)
            Log.e(
                WEB_API_LOG_TAG,
                "Web API $method $url status=${response.code} body=$preview"
            )
            HttpResult(
                statusCode = response.code,
                body = body,
                bodyPreview = preview
            )
        }
    }

    private fun throwStatus(endpoint: String, result: HttpResult): Nothing {
        if (result.statusCode == 401) {
            throw UnauthorizedException(
                statusCode = result.statusCode,
                bodyPreview = result.bodyPreview
            )
        }
        if (isNoActiveDevice(result.statusCode, result.bodyPreview)) {
            throw NoActiveDeviceException(
                statusCode = result.statusCode,
                bodyPreview = result.bodyPreview
            )
        }
        throw UnexpectedStatusException(
            statusCode = result.statusCode,
            bodyPreview = result.bodyPreview,
            endpoint = endpoint
        )
    }

    private fun isNoActiveDevice(statusCode: Int, body: String): Boolean {
        if (statusCode != 404) return false
        return body.contains("NO_ACTIVE_DEVICE", ignoreCase = true) ||
            body.contains("No active device", ignoreCase = true)
    }

    private data class HttpResult(
        val statusCode: Int,
        val body: String,
        val bodyPreview: String
    )
}
