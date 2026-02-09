package com.gitster.dj

import android.annotation.SuppressLint
import android.util.Size
import androidx.camera.core.CameraSelector
import androidx.camera.core.ImageAnalysis
import androidx.camera.core.ImageProxy
import androidx.camera.core.Preview
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.remember
import androidx.compose.runtime.mutableStateOf
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalLifecycleOwner
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.content.ContextCompat
import com.google.mlkit.vision.barcode.BarcodeScannerOptions
import com.google.mlkit.vision.barcode.BarcodeScanning
import com.google.mlkit.vision.barcode.common.Barcode
import com.google.mlkit.vision.common.InputImage
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Rectángulo normalizado (0..1) sobre la imagen (ya rotada por MLKit).
 * Sirve para limitar el escaneo a una zona central tipo “visor”.
 */
data class NormalizedRect(
    val left: Float,
    val top: Float,
    val right: Float,
    val bottom: Float,
) {
    fun contains(x: Float, y: Float): Boolean = (x in left..right) && (y in top..bottom)
}

@Composable
fun QrScannerPreview(
    modifier: Modifier,
    enabled: Boolean,
    roi: NormalizedRect? = null,
    onQr: (String) -> Unit
) {
    val context = LocalContext.current
    val lifecycleOwner = LocalLifecycleOwner.current

    val cameraProviderState = remember { mutableStateOf<ProcessCameraProvider?>(null) }
    val executor: ExecutorService = remember { Executors.newSingleThreadExecutor() }

    // Evita disparos múltiples por el mismo QR mientras sigues apuntando.
    val lastEmitAt = remember { AtomicLong(0L) }
    val lastValue = remember { AtomicReference<String?>(null) }
    val cooldownMs = 900L

    DisposableEffect(Unit) {
        onDispose {
            runCatching { cameraProviderState.value?.unbindAll() }
            executor.shutdown()
        }
    }

    AndroidView(
        modifier = modifier,
        factory = { ctx ->
            PreviewView(ctx).apply {
                implementationMode = PreviewView.ImplementationMode.COMPATIBLE
            }
        },
        update = { previewView ->
            val existingProvider = cameraProviderState.value
            if (!enabled) {
                runCatching { existingProvider?.unbindAll() }
                return@AndroidView
            }

            val cameraProviderFuture = ProcessCameraProvider.getInstance(context)
            cameraProviderFuture.addListener({
                val cameraProvider = cameraProviderFuture.get()
                cameraProviderState.value = cameraProvider

                val preview = Preview.Builder()
                    .build()
                    .also { it.setSurfaceProvider(previewView.surfaceProvider) }

                val options = BarcodeScannerOptions.Builder()
                    .setBarcodeFormats(Barcode.FORMAT_QR_CODE)
                    .build()
                val scanner = BarcodeScanning.getClient(options)

                val analysis = ImageAnalysis.Builder()
                    .setTargetResolution(Size(1280, 720))
                    .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
                    .build()

                analysis.setAnalyzer(executor) { imageProxy ->
                    analyzeFrame(scanner = scanner, imageProxy = imageProxy, roi = roi) { raw ->
                        val now = System.currentTimeMillis()
                        val prev = lastValue.get()
                        val prevAt = lastEmitAt.get()
                        if (raw == prev && (now - prevAt) < cooldownMs) return@analyzeFrame

                        lastValue.set(raw)
                        lastEmitAt.set(now)

                        // Importante: saltamos al hilo principal porque `onQr` suele mutar estado Compose.
                        ContextCompat.getMainExecutor(context).execute {
                            runCatching { onQr(raw) }
                        }
                    }
                }

                try {
                    cameraProvider.unbindAll()
                    cameraProvider.bindToLifecycle(
                        lifecycleOwner,
                        CameraSelector.DEFAULT_BACK_CAMERA,
                        preview,
                        analysis
                    )
                } catch (_: Throwable) {
                    // no crashear
                }
            }, ContextCompat.getMainExecutor(context))
        }
    )
}

@SuppressLint("UnsafeOptInUsageError")
private fun analyzeFrame(
    scanner: com.google.mlkit.vision.barcode.BarcodeScanner,
    imageProxy: ImageProxy,
    roi: NormalizedRect?,
    onQr: (String) -> Unit
) {
    val mediaImage = imageProxy.image
    if (mediaImage == null) {
        imageProxy.close()
        return
    }

    val image = InputImage.fromMediaImage(mediaImage, imageProxy.imageInfo.rotationDegrees)

    val rotation = imageProxy.imageInfo.rotationDegrees
    val widthUpright = if (rotation == 90 || rotation == 270) mediaImage.height else mediaImage.width
    val heightUpright = if (rotation == 90 || rotation == 270) mediaImage.width else mediaImage.height

    scanner.process(image)
        .addOnSuccessListener { barcodes ->
            val match = barcodes.firstOrNull { barcode ->
                val raw = barcode.rawValue?.trim()
                if (raw.isNullOrBlank()) return@firstOrNull false

                // Si hay ROI, exigimos que el QR esté dentro (aprox. por el centro del bounding box)
                if (roi != null) {
                    val box = barcode.boundingBox ?: return@firstOrNull false
                    val cx = box.exactCenterX() / widthUpright.toFloat()
                    val cy = box.exactCenterY() / heightUpright.toFloat()
                    return@firstOrNull roi.contains(cx, cy)
                }
                true
            }

            val raw = match?.rawValue?.trim()
            if (!raw.isNullOrBlank()) onQr(raw)
        }
        .addOnCompleteListener {
            imageProxy.close()
        }
}
