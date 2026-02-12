package com.gitster.dj

import android.util.Base64
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.SecureRandom

object SpotifyPkce {
    private val secureRandom = SecureRandom()

    fun generateCodeVerifier(): String {
        val bytes = ByteArray(64)
        secureRandom.nextBytes(bytes)
        return base64Url(bytes).take(128).padEnd(43, 'A')
    }

    fun codeChallengeS256(verifier: String): String {
        val digest = MessageDigest.getInstance("SHA-256")
            .digest(verifier.toByteArray(StandardCharsets.US_ASCII))
        return base64Url(digest)
    }

    fun randomState(): String {
        val bytes = ByteArray(16)
        secureRandom.nextBytes(bytes)
        return base64Url(bytes)
    }

    private fun base64Url(bytes: ByteArray): String {
        return Base64.encodeToString(
            bytes,
            Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING
        )
    }
}
