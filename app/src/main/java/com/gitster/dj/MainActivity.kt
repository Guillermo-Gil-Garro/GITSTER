package com.gitster.dj

import android.content.Intent
import android.os.Bundle
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.material3.Surface
import com.gitster.dj.ui.theme.GITSTERTheme

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        SpotifyAuthManager.initialize(applicationContext)
        Log.e(
            "GITSTER_SPOTIFY",
            "SMOKE MainActivity onCreate v=" + BuildConfig.VERSION_NAME +
                "(" + BuildConfig.VERSION_CODE + ") debug=" + BuildConfig.DEBUG
        )
        handleSpotifyRedirect(intent)

        val repo = DeckRepository(
            context = applicationContext,
            assetFileNames = listOf("deck.json", "deck_starter.json")
        )

        setContent {
            GITSTERTheme {
                Surface {
                    GitsterApp(
                        repo = repo,
                        rulesUrl = AppLinks.RULES_URL
                    )
                }
            }
        }
    }

    override fun onResume() {
        super.onResume()
        Log.e("GITSTER_SPOTIFY", "SMOKE MainActivity onResume")
    }

    override fun onStart() {
        super.onStart()
        Log.e(TAG, "ACT onStart")
    }

    override fun onPause() {
        super.onPause()
        Log.e(TAG, "ACT onPause")
    }

    override fun onStop() {
        super.onStop()
        Log.e(TAG, "ACT onStop")
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        SpotifyAuthManager.initialize(applicationContext)
        Log.e("GITSTER_SPOTIFY", "SMOKE MainActivity onNewIntent data=" + intent.dataString)
        setIntent(intent)
        handleSpotifyRedirect(intent)
    }

    private fun handleSpotifyRedirect(intent: Intent?) {
        val data = intent?.data
        Log.e("GITSTER_SPOTIFY", "MainActivity handleRedirect intent data=$data")
        val isCallback = data?.scheme.equals("gitster", ignoreCase = true) &&
            data?.host.equals("callback", ignoreCase = true)
        if (isCallback && intent != null) {
            SpotifyAuthManager.handleRedirectIntent(intent)
        }
    }
}
