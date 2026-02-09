package com.gitster.dj.ui.theme

import android.os.Build
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.dynamicDarkColorScheme
import androidx.compose.material3.dynamicLightColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext

private val DarkColorScheme = darkColorScheme(
    primary = GitsterCyan,
    onPrimary = Color.Black,
    secondary = GitsterMagenta,
    onSecondary = Color.Black,
    tertiary = GitsterViolet,
    onTertiary = Color.Black,

    background = GitsterBg0,
    onBackground = GitsterInk,
    surface = GitsterPanel,
    onSurface = GitsterInk,
    surfaceVariant = GitsterPanel2,
    onSurfaceVariant = GitsterInk,
    outline = Color(0x22FFFFFF)
)

private val LightColorScheme = lightColorScheme(
    primary = GitsterCyan,
    onPrimary = Color.Black,
    secondary = GitsterMagenta,
    onSecondary = Color.Black,
    tertiary = GitsterViolet,
    onTertiary = Color.Black,

    background = GitsterBg1,
    onBackground = GitsterInk,
    surface = GitsterPanel,
    onSurface = GitsterInk,
    surfaceVariant = GitsterPanel2,
    onSurfaceVariant = GitsterInk,
    outline = Color(0x22FFFFFF)

    /* Other default colors to override
    background = Color(0xFFFFFBFE),
    surface = Color(0xFFFFFBFE),
    onPrimary = Color.White,
    onSecondary = Color.White,
    onTertiary = Color.White,
    onBackground = Color(0xFF1C1B1F),
    onSurface = Color(0xFF1C1B1F),
    */
)

@Composable
fun GITSTERTheme(
    darkTheme: Boolean = isSystemInDarkTheme(),
    // Dynamic color is available on Android 12+
    // Para mantener el look & feel "neón" consistente, lo desactivamos por defecto.
    dynamicColor: Boolean = false,
    content: @Composable () -> Unit
) {
    val colorScheme = when {
        dynamicColor && Build.VERSION.SDK_INT >= Build.VERSION_CODES.S -> {
            val context = LocalContext.current
            if (darkTheme) dynamicDarkColorScheme(context) else dynamicLightColorScheme(context)
        }

        darkTheme -> DarkColorScheme
        else -> LightColorScheme
    }

    MaterialTheme(
        colorScheme = colorScheme,
        typography = Typography,
        content = content
    )
}