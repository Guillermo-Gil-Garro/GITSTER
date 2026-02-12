from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import yaml


ROOT = Path(__file__).resolve().parents[1]
EXPANSIONS_DIR = ROOT / "pipeline" / "config" / "expansions"

SCRIPT_PATHS: Dict[str, Path] = {
    "export": ROOT / "pipeline" / "spotify" / "export" / "parse_yourlibrary_export.py",
    "instances": ROOT / "pipeline" / "spotify" / "export" / "build_instances.py",
    "canonicalize": ROOT / "pipeline" / "canonicalize" / "canonicalize_and_registry.py",
    "years": ROOT / "pipeline" / "enrich" / "enrich_years.py",
    "deck": ROOT / "pipeline" / "deck" / "build_deck.py",
    "cards_preview": ROOT / "pipeline" / "cards" / "render_card_preview.py",
    "cards_sheets": ROOT / "pipeline" / "cards" / "render_print_sheets.py",
}

PIPELINE_ALL_ORDER: List[str] = [
    "export",
    "instances",
    "canonicalize",
    "years",
    "deck",
    "cards_preview",
    "cards_sheets",
]

EXPANSION_AWARE = {"instances", "canonicalize", "years", "deck"}
CARDS_COMMANDS = {"cards_preview", "cards_sheets"}


def split_passthrough(argv: Sequence[str]) -> Tuple[List[str], List[str]]:
    if "--" not in argv:
        return list(argv), []
    idx = list(argv).index("--")
    return list(argv[:idx]), list(argv[idx + 1 :])


def find_default_expansion_yaml() -> Path | None:
    if not EXPANSIONS_DIR.exists():
        return None

    candidates = sorted(
        list(EXPANSIONS_DIR.glob("*.yaml")) + list(EXPANSIONS_DIR.glob("*.yml")),
        key=lambda p: str(p).lower(),
    )
    return candidates[0] if candidates else None


def load_yaml_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[run.py] warning: cannot read config {path}: {exc}", file=sys.stderr)
        return {}

    if isinstance(data, dict):
        return data
    return {}


def has_flag(args: Sequence[str], flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in args)


def resolve_runtime_config(expansion_arg: str | None, config_arg: str | None) -> Tuple[str, Path, Dict[str, Any]]:
    default_yaml = find_default_expansion_yaml()

    if config_arg:
        config_path = Path(config_arg)
        cfg = load_yaml_config(config_path)
        expansion = (expansion_arg or str(cfg.get("expansion", "")).strip() or config_path.stem or "I")
        return expansion, config_path, cfg

    if expansion_arg:
        expansion = expansion_arg
        config_path = EXPANSIONS_DIR / f"{expansion}.yaml"
        return expansion, config_path, load_yaml_config(config_path)

    if default_yaml:
        cfg = load_yaml_config(default_yaml)
        expansion = str(cfg.get("expansion", "")).strip() or default_yaml.stem or "I"
        config_path = EXPANSIONS_DIR / f"{expansion}.yaml"
        if not config_path.exists():
            config_path = default_yaml
        return expansion, config_path, cfg

    expansion = "I"
    config_path = EXPANSIONS_DIR / f"{expansion}.yaml"
    return expansion, config_path, {}


def extra_args_for_stage(command: str, expansion: str, cfg: Dict[str, Any], passthrough: Sequence[str]) -> List[str]:
    extra: List[str] = []

    if command in EXPANSION_AWARE and not has_flag(passthrough, "--expansion"):
        extra.extend(["--expansion", expansion])

    if command == "deck":
        deck_cfg = cfg.get("deck")
        if isinstance(deck_cfg, dict):
            if not has_flag(passthrough, "--limit") and "limit" in deck_cfg:
                extra.extend(["--limit", str(deck_cfg["limit"])])
            if not has_flag(passthrough, "--max-per-album") and "max_per_album" in deck_cfg:
                extra.extend(["--max-per-album", str(deck_cfg["max_per_album"])])

    if command in CARDS_COMMANDS and not has_flag(passthrough, "--deck"):
        extra.extend(["--deck", f"pipeline/data/processed/deck_{expansion}.csv"])

    return extra


def build_command(command: str, expansion: str, cfg: Dict[str, Any], passthrough: Sequence[str]) -> List[str]:
    script = SCRIPT_PATHS[command]
    cmd = [sys.executable, str(script)]
    cmd.extend(extra_args_for_stage(command, expansion, cfg, passthrough))
    cmd.extend(list(passthrough))
    return cmd


def run_stage(command: str, expansion: str, cfg: Dict[str, Any], passthrough: Sequence[str], dry_run: bool) -> int:
    cmd = build_command(command, expansion, cfg, passthrough)
    print(subprocess.list2cmdline(cmd))

    if dry_run:
        return 0

    completed = subprocess.run(cmd, cwd=str(ROOT))
    return int(completed.returncode)


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--expansion",
        default=None,
        help="Expansion code. Default: loaded from YAML config when available.",
    )
    common.add_argument(
        "--config",
        default=None,
        help="Path to expansion YAML. Default: pipeline/config/expansions/{EXP}.yaml",
    )
    common.add_argument(
        "--dry-run",
        action="store_true",
        help="Print exact command(s) without executing them.",
    )

    parser = argparse.ArgumentParser(
        description="Stable wrapper for pipeline stage scripts.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sub_help = {
        "export": "Run Spotify export parser.",
        "instances": "Build instances CSVs.",
        "canonicalize": "Build linked instances and canonical registry.",
        "years": "Enrich canonical songs with year metadata.",
        "deck": "Build deck CSV/JSON.",
        "cards_preview": "Render one card preview PDF.",
        "cards_sheets": "Render print sheets PDFs.",
        "all": "Run all stages in order.",
    }

    for name in list(SCRIPT_PATHS.keys()) + ["all"]:
        subparsers.add_parser(name, parents=[common], help=sub_help[name])

    return parser


def main() -> int:
    parser = build_parser()
    args_no_passthrough, passthrough = split_passthrough(sys.argv[1:])
    args = parser.parse_args(args_no_passthrough)

    expansion, config_path, cfg = resolve_runtime_config(args.expansion, args.config)

    if args.config and not config_path.exists():
        parser.error(f"--config not found: {config_path}")

    if args.command == "all" and passthrough:
        parser.error("Passthrough with 'all' is not supported. Run stages individually to pass extra args.")

    stages = PIPELINE_ALL_ORDER if args.command == "all" else [args.command]

    for stage in stages:
        stage_passthrough: Sequence[str] = passthrough if stage == args.command else []
        rc = run_stage(
            command=stage,
            expansion=expansion,
            cfg=cfg,
            passthrough=stage_passthrough,
            dry_run=bool(args.dry_run),
        )
        if rc != 0:
            return rc

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
