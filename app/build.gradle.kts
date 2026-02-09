plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
    // Kotlin 2.0+ requires the Compose compiler plugin when Compose is enabled
    alias(libs.plugins.compose.compiler)
}

import java.util.Properties

android {
    namespace = "com.gitster.dj"
    // 36 (preview) suele romper en máquinas con SDKs estándar.
    // Para MVP, apunta a un SDK estable instalado normalmente.
    compileSdk = 35

    defaultConfig {
        applicationId = "com.gitster.dj"
        minSdk = 24
        targetSdk = 35
        // Bump para forzar update del launcher icon en dispositivos que cachean agresivo.
        versionCode = 3
        versionName = "1.2"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        vectorDrawables {
            useSupportLibrary = true
        }

        // Spotify (MVP-ready): se inyecta desde local.properties para que puedas pegar el Client ID
        // sin tocar código. Si no existe, queda vacío y la app funciona igualmente.
        val localProps = Properties()
        val localPropsFile = rootProject.file("local.properties")
        if (localPropsFile.exists()) {
            runCatching { localPropsFile.inputStream().use { localProps.load(it) } }
        }

        val spotifyClientId = (localProps.getProperty("SPOTIFY_CLIENT_ID") ?: "").trim()
        val spotifyRedirectUri = (localProps.getProperty("SPOTIFY_REDIRECT_URI") ?: "gitster://callback").trim()

        buildConfigField("String", "SPOTIFY_CLIENT_ID", "\"${spotifyClientId}\"")
        buildConfigField("String", "SPOTIFY_REDIRECT_URI", "\"${spotifyRedirectUri}\"")
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }

    buildFeatures {
        compose = true
        buildConfig = true
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    kotlinOptions {
        jvmTarget = "11"
    }

    packaging {
        resources {
            excludes += "/META-INF/{AL2.0,LGPL2.1}"
        }
    }
}

dependencies {
    val camerax = "1.3.4"

    // Android basics
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.activity.compose)

    // Compose
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.compose.ui)
    implementation(libs.androidx.compose.ui.graphics)
    implementation(libs.androidx.compose.ui.tooling.preview)
    implementation(libs.androidx.compose.material3)
    debugImplementation(libs.androidx.compose.ui.tooling)
    debugImplementation(libs.androidx.compose.ui.test.manifest)

    // CameraX
    implementation("androidx.camera:camera-core:$camerax")
    implementation("androidx.camera:camera-camera2:$camerax")
    implementation("androidx.camera:camera-lifecycle:$camerax")
    implementation("androidx.camera:camera-view:$camerax")

    // ML Kit QR
    implementation(libs.mlkit.barcode.scanning)

    // Deck parsing + coroutines
    implementation("com.google.code.gson:gson:2.10.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")

    // Fallback para abrir links (Spotify)
    implementation("androidx.browser:browser:1.8.0")

    // Spotify App Remote (AAR local en app/libs/)
    // MVP: desactivado para no introducir dependencias/errores de build antes de tiempo.
    // (cuando toque, lo reactivamos y añadimos sus deps/transitives si hiciera falta)
    // implementation(files("libs/spotify-app-remote-release-0.8.0.aar"))

    // Tests
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.compose.ui.test.junit4)
}
