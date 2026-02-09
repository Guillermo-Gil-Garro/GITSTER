package com.gitster.dj

import android.Manifest
import android.graphics.Rect
import android.os.SystemClock
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.camera.core.*
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalLifecycleOwner
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.content.ContextCompat
import com.google.mlkit.vision.barcode.common.Barcode
import com.google.mlkit.vision.barcode.BarcodeScannerOptions
import com.google.mlkit.vision.barcode.BarcodeScanning
import com.google.mlkit.vision.common.InputImage
import kotlin.math.max
import kotlin.math.min

@Composable
fun ScanScreen(
    onScanned: (String) -> Unit,
    onClose: () -> Unit,
) {
    val context = LocalContext.current
    val lifecycleOwner = LocalLifecycleOwner.current

    var hasCameraPermission by remember { mutableStateOf(false) }
    val permissionLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.RequestPermission()
    ) { granted ->
        hasCameraPermission = granted
        if (!granted) onClose()
    }

    LaunchedEffect(Unit) {
        permissionLauncher.launch(Manifest.permission.CAMERA)
    }

    if (!hasCameraPermission) {
        Box(Modifier.fillMaxSize().background(Color.Black)) {
            Text(
                "Se necesita permiso de cámara",
                color = Color.White,
                modifier = Modifier.align(Alignment.Center)
            )
        }
        return
    }

    // Evita disparar `onScanned()` en bucle, pero SIN bloquear la cámara para siempre.
    // (Antes usábamos un AtomicBoolean que quedaba en `true` si la resolución fallaba
    // y el usuario se quedaba en el escáner -> el scanner quedaba "muerto".)
    var lastFireMs by remember { mutableStateOf(0L) }
    val fireCooldownMs = 900L

    // Debounce: requiere mantener el mismo QR “apuntado” un rato
    var candidateValue by remember { mutableStateOf<String?>(null) }
    var candidateSinceMs by remember { mutableStateOf(0L) }

    Box(Modifier.fillMaxSize()) {

        AndroidView(
            modifier = Modifier.fillMaxSize(),
            factory = { ctx ->
                PreviewView(ctx).apply {
                    scaleType = PreviewView.ScaleType.FILL_CENTER
                }
            },
            update = { previewView ->
                val cameraProviderFuture = ProcessCameraProvider.getInstance(context)
                cameraProviderFuture.addListener({
                    val cameraProvider = cameraProviderFuture.get()

                    val preview = Preview.Builder().build().also {
                        it.setSurfaceProvider(previewView.surfaceProvider)
                    }

                    val analysis = ImageAnalysis.Builder()
                        .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
                        .build()

                    val options = BarcodeScannerOptions.Builder()
                        .setBarcodeFormats(Barcode.FORMAT_QR_CODE)
                        .build()
                    val scanner = BarcodeScanning.getClient(options)

                    analysis.setAnalyzer(ContextCompat.getMainExecutor(context)) { imageProxy ->
                        val mediaImage = imageProxy.image
                        if (mediaImage == null) {
                            imageProxy.close()
                            return@setAnalyzer
                        }

                        val rotation = imageProxy.imageInfo.rotationDegrees
                        val inputImage = InputImage.fromMediaImage(mediaImage, rotation)

                        scanner.process(inputImage)
                            .addOnSuccessListener { barcodes ->
                                if (barcodes.isEmpty()) return@addOnSuccessListener

                                val best = barcodes
                                    .filter { it.rawValue != null && it.boundingBox != null }
                                    .maxByOrNull { boxArea(it.boundingBox!!) }
                                    ?: return@addOnSuccessListener

                                val raw = best.rawValue ?: return@addOnSuccessListener
                                val box = best.boundingBox ?: return@addOnSuccessListener

                                val (iw, ih) = effectiveImageSize(imageProxy.width, imageProxy.height, rotation)

                                // “Acotar” (tipo vídeo): solo aceptar si el QR cae dentro de la ventana.
                                // Implementación MVP: gating por bounding box (sin ROI real todavía).
                                val ok = passesViewfinderGate(box, iw, ih)
                                if (!ok) {
                                    candidateValue = null
                                    candidateSinceMs = 0L
                                    return@addOnSuccessListener
                                }

                                val now = SystemClock.elapsedRealtime()

                                if (candidateValue == raw) {
                                    val heldFor = now - candidateSinceMs
                                    if (heldFor >= 420 && (now - lastFireMs) >= fireCooldownMs) {
                                        lastFireMs = now
                                        onScanned(raw)
                                    }
                                } else {
                                    candidateValue = raw
                                    candidateSinceMs = now
                                }
                            }
                            .addOnFailureListener {
                                // ignorar
                            }
                            .addOnCompleteListener {
                                imageProxy.close()
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
                    } catch (_: Exception) {
                        // Si falla, cerramos para no dejar la app colgada
                        onClose()
                    }
                }, ContextCompat.getMainExecutor(context))
            }
        )

        // Overlay + UI
        ViewfinderOverlay(
            modifier = Modifier.fillMaxSize(),
            onClose = onClose
        )
    }
}

@Composable
private fun ViewfinderOverlay(
    modifier: Modifier = Modifier,
    onClose: () -> Unit
) {
    val dim = Color(0xAA000000)
    val frameWFrac = 0.78f
    val frameAspect = 1.0f // cuadrado (ajústalo si en el vídeo es más alto)
    val shape = RoundedCornerShape(26.dp)

    BoxWithConstraints(modifier) {
        val w = maxWidth
        val h = maxHeight

        val frameW = w * frameWFrac
        val frameH = frameW / frameAspect

        val left = (w - frameW) / 2
        val top = (h - frameH) / 2
        val bottomH = h - (top + frameH)

        // Capas oscurecidas alrededor
        Box(Modifier.fillMaxWidth().height(top).background(dim).align(Alignment.TopStart))
        Box(Modifier.fillMaxWidth().height(bottomH).background(dim).align(Alignment.BottomStart))
        Box(
            Modifier.width(left).fillMaxHeight()
                .padding(top = top, bottom = bottomH)
                .background(dim)
                .align(Alignment.TopStart)
        )
        Box(
            Modifier.width(left).fillMaxHeight()
                .padding(top = top, bottom = bottomH)
                .background(dim)
                .align(Alignment.TopEnd)
        )

        // Marco neon
        Box(
            Modifier
                .offset(x = left, y = top)
                .size(frameW, frameH)
                .border(
                    width = 3.dp,
                    brush = Brush.linearGradient(
                        listOf(
                            Color(0xFFFF2BD6), // magenta
                            Color(0xFF00D1FF), // cyan
                            Color(0xFFFFD400)  // amarillo
                        )
                    ),
                    shape = shape
                )
        )

        // Top bar (tipo vídeo)
        Row(
            Modifier
                .fillMaxWidth()
                .padding(14.dp),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                "GITSTER",
                color = Color.White,
                fontWeight = FontWeight.Black
            )
            TextButton(onClick = onClose) {
                Text("Salir", color = Color.White, fontWeight = FontWeight.SemiBold)
            }
        }

        // Instrucción
        Text(
            "Apunta al QR dentro del marco",
            color = Color.White,
            modifier = Modifier
                .align(Alignment.Center)
                .offset(y = (-frameH / 2) - 30.dp)
        )
    }
}

private fun boxArea(r: Rect): Int = r.width() * r.height()

private fun effectiveImageSize(w: Int, h: Int, rotation: Int): Pair<Int, Int> {
    return if (rotation == 90 || rotation == 270) h to w else w to h
}

/**
 * Gate central (aprox): obliga a “apuntar”.
 * Ajusta los rangos si quieres un marco más estricto.
 */
private fun passesViewfinderGate(box: Rect, imageW: Int, imageH: Int): Boolean {
    // Mantener estos parámetros alineados con el overlay.
    val frameWFrac = 0.78f
    val frameAspect = 1.0f // 1 = cuadrado

    // Convertimos la "ventana" (definida en términos de ancho de pantalla) a fracciones aproximadas
    // sobre la imagen analizada. Esto NO es una transformación perfecta (PreviewView usa FILL_CENTER),
    // pero funciona suficientemente bien como MVP para obligar a apuntar.
    val aspect = imageW.toFloat() / imageH.toFloat()
    val frameHFrac = (frameWFrac * aspect / frameAspect).coerceIn(0.20f, 0.92f)

    val left = ((1f - frameWFrac) / 2f).coerceIn(0f, 0.5f)
    val right = (1f - left).coerceIn(0.5f, 1f)
    val top = ((1f - frameHFrac) / 2f).coerceIn(0f, 0.5f)
    val bottom = (1f - top).coerceIn(0.5f, 1f)

    val cx = box.exactCenterX() / imageW.toFloat()
    val cy = box.exactCenterY() / imageH.toFloat()

    val bw = box.width() / imageW.toFloat()
    val bh = box.height() / imageH.toFloat()
    val bmin = min(bw, bh)

    // Además de que el centro caiga dentro, evitamos aceptar QRs demasiado "raspando" el borde.
    val margin = 0.03f
    val centerOk = cx in (left + margin)..(right - margin) && cy in (top + margin)..(bottom - margin)

    // Tamaño mínimo: si está muy lejos, suele ser pequeño y no queremos que dispare.
    val sizeOk = bmin > 0.09f

    return centerOk && sizeOk
}
