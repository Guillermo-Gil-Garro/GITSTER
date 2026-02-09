package com.gitster.dj

/**
 * Config de Spotify inyectada desde Gradle.
 *
 * Para activar:
 * 1) Abre `local.properties` (en la raíz del proyecto)
 * 2) Añade:
 *    SPOTIFY_CLIENT_ID=... 
 *    SPOTIFY_REDIRECT_URI=gitster://callback
 */
object SpotifyConfig {
    val clientId: String = BuildConfig.SPOTIFY_CLIENT_ID
    val redirectUri: String = BuildConfig.SPOTIFY_REDIRECT_URI

    fun isConfigured(): Boolean = clientId.isNotBlank() && redirectUri.isNotBlank()
}
