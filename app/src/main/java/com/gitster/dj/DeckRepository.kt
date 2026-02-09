package com.gitster.dj

import android.content.Context
import com.google.gson.Gson
import com.google.gson.annotations.SerializedName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Modelo tolerante a variaciones del JSON del deck.
 * - Soporta year como 1963.0 (número) o "1963" (string)
 * - Soporta claves alternativas generadas por el pipeline
 */
data class DeckCard(
    @SerializedName(value = "card_id", alternate = ["canonical_id", "id"])
    val cardId: String? = null,

    @SerializedName(value = "track_id", alternate = ["spotify_track_id", "spotifyTrackId", "trackId"])
    val trackId: String? = null,

    @SerializedName(value = "title_display", alternate = ["title", "name", "title_canon", "titleCanon"])
    val titleDisplay: String? = null,

    @SerializedName(value = "artists_display", alternate = ["artists", "artist", "artists_canon", "artistsCanon"])
    val artistsDisplay: String? = null,

    // En algunos decks: year = 1963.0, year_int = "1963".
    // Si lo tipamos como String, Gson acepta tanto number como string.
    @SerializedName(value = "year_int", alternate = ["year", "release_year", "releaseYear"])
    val year: String? = null,

    @SerializedName(value = "spotify_url", alternate = ["spotifyUrl"])
    val spotifyUrl: String? = null,

    @SerializedName(value = "spotify_uri", alternate = ["spotifyUri"])
    val spotifyUri: String? = null,

    @SerializedName(value = "qr_payload", alternate = ["qrPayload"])
    val qrPayload: String? = null,

    @SerializedName(value = "expansion", alternate = ["exp", "expansion_code", "first_seen_expansion", "expansionCode"])
    val expansion: String? = null,

    @SerializedName(value = "owner", alternate = ["deck_owner", "owners", "deckOwner"])
    val owner: String? = null
) {
    fun yearAsIntOrNull(): Int? {
        val v = year?.trim().orEmpty()
        if (v.isBlank()) return null
        val head = v.substringBefore('.').trim() // "1963.0" -> "1963"
        return head.toIntOrNull()
    }

    fun titleForUi(): String = titleDisplay?.trim().takeIf { !it.isNullOrBlank() } ?: "(sin título)"

    fun artistsForUi(): String = artistsDisplay?.trim().takeIf { !it.isNullOrBlank() } ?: "(sin artistas)"
}

data class ScanResolution(
    val raw: String,
    val kind: Kind,
    val parsedOwner: String? = null,
    val parsedExpansion: String? = null,
    val parsedCardId: String? = null,
    val parsedTrackId: String? = null,
    val card: DeckCard? = null,
    val error: String? = null
) {
    enum class Kind { GITSTER_V1, SPOTIFY, PLAIN, UNKNOWN, ERROR }
}

data class DeckLoadInfo(
    val assetFileName: String,
    val cardCount: Int
)

class DeckRepository(
    private val context: Context,
    private val assetFileNames: List<String> = listOf("deck.json", "deck_starter.json")
) {
    private var loaded = false
    private var loadInfo: DeckLoadInfo? = null

    private var byCardId: Map<String, DeckCard> = emptyMap()
    private var byTrackId: Map<String, DeckCard> = emptyMap()
    private var bySpotifyUrl: Map<String, DeckCard> = emptyMap()
    private var bySpotifyUri: Map<String, DeckCard> = emptyMap()

    fun getLoadInfo(): DeckLoadInfo? = loadInfo

    suspend fun loadDeckIfNeeded(): DeckLoadInfo? {
        if (loaded) return loadInfo

        return withContext(Dispatchers.IO) {
            val (usedName, cards) = loadFirstAvailableDeck() ?: return@withContext null

            if (cards.isEmpty()) return@withContext null

            byCardId = cards
                .mapNotNull { c -> c.cardId?.trim()?.takeIf { it.isNotBlank() }?.let { it to c } }
                .toMap()

            byTrackId = cards
                .mapNotNull { c -> normalizeTrackId(c.trackId)?.let { it to c } }
                .toMap()

            bySpotifyUrl = cards
                .mapNotNull { c -> c.spotifyUrl?.trim()?.let { normalizeSpotifyUrl(it) }?.let { it to c } }
                .toMap()

            bySpotifyUri = cards
                .mapNotNull { c -> c.spotifyUri?.trim()?.let { normalizeSpotifyUri(it) }?.let { it to c } }
                .toMap()

            loaded = true
            loadInfo = DeckLoadInfo(assetFileName = usedName, cardCount = cards.size)
            loadInfo
        }
    }

    private fun loadFirstAvailableDeck(): Pair<String, List<DeckCard>>? {
        for (name in assetFileNames) {
            val cards = runCatching {
                val json = context.assets.open(name).bufferedReader().use { it.readText() }
                parseCards(json)
            }.getOrNull()

            if (!cards.isNullOrEmpty()) return name to cards
        }
        return null
    }

    fun resolveFromRaw(rawInput: String): ScanResolution {
        val raw = rawInput.trim()
        if (raw.isBlank()) {
            return ScanResolution(raw = rawInput, kind = ScanResolution.Kind.UNKNOWN, error = "Payload vacío")
        }

        // 1) Formato final: gitster:v1:<owner>:<expansion>:<card_id>
        if (raw.startsWith("gitster:v1:", ignoreCase = true)) {
            val parts = raw.split(":")

            val owner: String?
            val exp: String?
            val cardId: String?

            when (parts.size) {
                5 -> {
                    owner = parts[2].trim().ifBlank { null }
                    exp = parts[3].trim().ifBlank { null }
                    cardId = parts[4].trim().ifBlank { null }
                }
                4 -> {
                    // Compat: gitster:v1:<deck_or_owner>:<card_id>
                    owner = parts[2].trim().ifBlank { null }
                    exp = null
                    cardId = parts[3].trim().ifBlank { null }
                }
                3 -> {
                    owner = null
                    exp = null
                    cardId = parts[2].trim().ifBlank { null }
                }
                else -> {
                    owner = null
                    exp = null
                    cardId = null
                }
            }

            val found = cardId?.let { byCardId[it] }
            return ScanResolution(
                raw = raw,
                kind = ScanResolution.Kind.GITSTER_V1,
                parsedOwner = owner,
                parsedExpansion = exp,
                parsedCardId = cardId,
                card = found,
                error = if (cardId == null) "Formato gitster:v1 inválido" else null
            )
        }

        // 2) Compat: Spotify URL/URI
        val trackIdFromSpotify = extractTrackIdFromSpotify(raw)
        if (trackIdFromSpotify != null) {
            val found = findBySpotifyTrackIdOrLink(trackIdFromSpotify, raw)
            return ScanResolution(
                raw = raw,
                kind = ScanResolution.Kind.SPOTIFY,
                parsedTrackId = trackIdFromSpotify,
                card = found
            )
        }

        // 3) Si el raw parece un track_id "pelado" (22 chars)
        if (isLikelySpotifyTrackId(raw)) {
            val found = findBySpotifyTrackIdOrLink(raw, raw)
            return ScanResolution(
                raw = raw,
                kind = ScanResolution.Kind.SPOTIFY,
                parsedTrackId = raw,
                card = found
            )
        }

        // 4) Payload plano: asumir card_id directo
        val found = byCardId[raw]
        return ScanResolution(
            raw = raw,
            kind = if (found != null) ScanResolution.Kind.PLAIN else ScanResolution.Kind.UNKNOWN,
            parsedCardId = raw,
            card = found
        )
    }

    private fun findBySpotifyTrackIdOrLink(trackId: String, raw: String): DeckCard? {
        val tid = normalizeTrackId(trackId)
        val urlNorm = normalizeSpotifyUrl(raw)
        val uriNorm = normalizeSpotifyUri(raw)

        return (tid?.let { byTrackId[it] })
            ?: (urlNorm?.let { bySpotifyUrl[it] })
            ?: (uriNorm?.let { bySpotifyUri[it] })
    }

    private fun parseCards(json: String): List<DeckCard> {
        val gson = Gson()
        val trimmed = json.trimStart()

        return try {
            if (trimmed.startsWith("[")) {
                gson.fromJson(trimmed, Array<DeckCard>::class.java).toList()
            } else {
                // Soportar {"cards":[...]} o {"deck":[...]}
                val obj = gson.fromJson(trimmed, Map::class.java)
                val key = when {
                    obj.containsKey("cards") -> "cards"
                    obj.containsKey("deck") -> "deck"
                    else -> null
                }
                if (key != null) {
                    val innerJson = gson.toJson(obj[key])
                    gson.fromJson(innerJson, Array<DeckCard>::class.java).toList()
                } else {
                    emptyList()
                }
            }
        } catch (_: Throwable) {
            emptyList()
        }
    }

    private fun isLikelySpotifyTrackId(value: String): Boolean {
        return value.length == 22 && value.all { it.isLetterOrDigit() }
    }

    private fun normalizeTrackId(id: String?): String? {
        val v = id?.trim()?.takeIf { it.isNotBlank() } ?: return null
        if (v.startsWith("spotify:track:", ignoreCase = true)) {
            val inner = v.substringAfter("spotify:track:").trim()
            return inner.takeIf { it.isNotBlank() }
        }
        return v.takeIf { isLikelySpotifyTrackId(it) }
    }

    private fun extractTrackIdFromSpotify(raw: String): String? {
        val s = raw.trim()
        if (s.startsWith("spotify:track:", ignoreCase = true)) return normalizeTrackId(s)

        val marker = "open.spotify.com/track/"
        val idx = s.indexOf(marker, ignoreCase = true)
        if (idx >= 0) {
            val after = s.substring(idx + marker.length)
            val id = after.substringBefore("?").substringBefore("/").trim()
            return id.takeIf { isLikelySpotifyTrackId(it) }
        }
        return null
    }

    private fun normalizeSpotifyUrl(raw: String): String? {
        val tid = extractTrackIdFromSpotify(raw) ?: return null
        return "https://open.spotify.com/track/$tid"
    }

    private fun normalizeSpotifyUri(raw: String): String? {
        val tid = extractTrackIdFromSpotify(raw) ?: normalizeTrackId(raw) ?: return null
        return "spotify:track:$tid"
    }
}
