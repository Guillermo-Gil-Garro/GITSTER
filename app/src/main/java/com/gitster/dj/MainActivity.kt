package com.gitster.dj

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.material3.Surface
import com.gitster.dj.ui.theme.GITSTERTheme

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

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
}
