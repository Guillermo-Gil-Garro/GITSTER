package com.gitster.dj

object SpotifyConfig {
    val clientId: String = BuildConfig.SPOTIFY_CLIENT_ID
    val redirectUri: String = BuildConfig.SPOTIFY_REDIRECT_URI

    fun isConfigured(): Boolean = clientId.isNotBlank() && redirectUri.isNotBlank()
}
