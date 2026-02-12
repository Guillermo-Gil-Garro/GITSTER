# Pipeline CLI (oficial)

## Prerequisitos
- Windows + PowerShell.
- Activar entorno conda `ds`.
- `.env` opcional (solo si algun script lo necesita para API keys/tokens).

## Config base
- Expansion config por defecto: `pipeline/config/expansions/I.yaml`.
- `pipeline/run.py` intenta resolver `--expansion` desde YAML si existe.
- Puedes forzar un YAML distinto con `--config`.

## Comandos por etapa
- Export (`YourLibrary.json` -> CSV):
  - `python pipeline/run.py export -- --all`
  - `python pipeline/run.py export -- --owner Guille`

- Instances:
  - `python pipeline/run.py instances --expansion I -- --all`
  - `python pipeline/run.py instances --expansion I -- --owner Luks`

- Canonicalize + registry:
  - `python pipeline/run.py canonicalize --expansion I`
  - `python pipeline/run.py canonicalize --expansion I -- --owner Guille`

- Years:
  - `python pipeline/run.py years --expansion I`
  - `python pipeline/run.py years --expansion I -- --mode mb --max-items 200`

- Deck:
  - `python pipeline/run.py deck --expansion I`
  - `python pipeline/run.py deck --expansion I -- --limit 300 --max-per-album 3`

- Cards preview:
  - `python pipeline/run.py cards_preview --expansion I`
  - `python pipeline/run.py cards_preview --expansion I -- --card-id I-edd02c7d`

- Cards sheets:
  - `python pipeline/run.py cards_sheets --expansion I`
  - `python pipeline/run.py cards_sheets --expansion I -- --only 4x3_short,4x3_long`

- Pipeline completo:
  - `python pipeline/run.py all --expansion I`

## Passthrough (`--`)
Todo lo que vaya despues de `--` se reenvia tal cual al script de etapa:
- `python pipeline/run.py years --expansion I -- --mode hybrid --max-items 200`
- `python pipeline/run.py deck --expansion I -- --prefer-have-spotify-url`

## Politica de versionado
No se commitean:
- `pipeline/data/processed/**`
- `pipeline/reports/**`
- `pipeline/cache/**`
- `pipeline/data/raw/spotify_export/**` (privacidad)
- `.env`, `.env.*`
