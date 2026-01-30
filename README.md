# GITSTER 🎧🪩🎰

**GITSTER** es un juego tipo *timeline musical* con **economía de fichas y apuestas**: colocas canciones en orden cronológico, negocias riesgos, y ganas si consigues completar tu timeline antes que el resto.

➡️ **Reglas (web):** https://guillermo-gil-garro.github.io/GITSTER/  
📄 **Reglamento (PDF):** `docs/rules/` (en este repo)

---

## 🎯 ¿QUÉ ES ESTO?

- Un **juego de mesa** inspirado en el “coloca la canción en su año” con un toque extra de **gambling** 🎰  
- Un proyecto **Data + Design**:
  - **Pipeline**: playlists de :contentReference[oaicite:0]{index=0} → dataset → mazo (cartas)
  - **Diseño**: cartas imprimibles con estética club/neón
  - **App** (más adelante): soporte para escaneo/reproducción y gestión de mazos

---

## 🧩 QUÉ ENCUENTRAS EN ESTE REPO

- ✅ **Web de reglas** (GitHub Pages) para compartir con colegas
- ✅ Assets de la web (imágenes, estilos)
- 🛠️ (En progreso) Pipeline de datos y generación de mazo/cartas
- 🧠 (En progreso) PRD / prototipo de app

---

## 🎮 CÓMO SE JUEGA (RESUMEN)

1) **Construye tu timeline** colocando canciones en orden cronológico  
2) **Apuesta fichas** cuando toque: aquí es donde pasa la magia 🎰  
3) **Revela** y resuelve: si aciertas, avanzas; si fallas… se paga el precio  
4) **Gana** quien complete antes el objetivo de cartas/timeline según el reglamento

📌 **El detalle fino y las reglas exactas** están en la web de reglas:  
https://guillermo-gil-garro.github.io/GITSTER/

---

## 🗂️ ESTRUCTURA DEL REPO (RÁPIDA)

- `docs/` → **Sitio web** (lo que publica GitHub Pages)
- `docs/assets/` → imágenes/recursos de la web
- `docs/rules/` → reglamento y materiales del juego
- *(próximamente)* `src/`, `scripts/`, `notebooks/` → pipeline y generación del mazo

---

## 🌐 PUBLICAR / EDITAR LA WEB DE REGLAS (SIN MISTERIOS)

Este repo usa **:contentReference[oaicite:1]{index=1} Pages** apuntando a la carpeta `/docs`.

- La página principal **DEBE** llamarse: `docs/index.html`
- Las imágenes deben estar en rutas correctas (ej. `docs/assets/...`)

### Si quieres editar algo rápido
1. Entra a `docs/index.html`
2. Pulsa el ✏️ (Edit)
3. Cambia lo que quieras
4. **Commit changes** ✅

---

## 🧪 ROADMAP (LO QUE VIENE)

**MVP 1 — Web + reglas (hecho ✅)**
- [x] Publicación de reglas en Pages
- [x] Estructura base del repo

**MVP 2 — Pipeline de datos**
- [ ] Ingesta de playlists
- [ ] Export a dataset único (CSV/Parquet)
- [ ] Reporte de calidad (duplicados, fechas faltantes, etc.)

**MVP 3 — Cartas**
- [ ] Plantilla visual (print + PNG)
- [ ] Generación batch desde dataset
- [ ] Control de calidad (márgenes, legibilidad)

**V2 — App**
- [ ] Escaneo / reproducción / reveal
- [ ] Gestión de mazos personalizados
- [ ] Partidas y registro de apuestas (si aplica)

---

## 🤝 CONTRIBUIR

¿Quieres ayudar? Bienvenido/a 🎧🪩  
- Abre un **Issue** con sugerencias, bugs o ideas de balance
- O propone cambios mediante PR (si te doy acceso / si procede)

---

## ⚠️ NOTA LEGAL / DISCLAIMER

Proyecto fan/experimental. No está afiliado ni respaldado por Spotify/GitHub ni por ninguna marca relacionada.  
Las reglas, assets y el diseño de este proyecto son propios del repo (salvo que se indique lo contrario).

---

## 📬 CONTACTO

Si eres colega y quieres meter tus playlists en el mazo: escríbeme y te digo el formato/flujo (sin subir datos sensibles al repo).
