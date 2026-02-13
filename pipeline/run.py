from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
from collections import Counter
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

PIPELINE_ALL_CORE: List[str] = [
    "export",
    "instances",
    "canonicalize",
    "years",
    "deck",
]

# cards_preview se mantiene como subcomando explícito, pero no se ejecuta por defecto en all --with-cards.
CARDS_STAGES: List[str] = ["cards_sheets"]
EXPANSION_AWARE = {"instances", "canonicalize", "years", "deck"}


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
        expansion = expansion_arg or str(cfg.get("expansion", "")).strip() or config_path.stem or "I"
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


def deck_flags_from_config(cfg: Dict[str, Any], passthrough: Sequence[str]) -> List[str]:
    extra: List[str] = []
    deck_cfg = cfg.get("deck")
    if not isinstance(deck_cfg, dict):
        return extra

    mappings = [
        ("limit", "--limit"),
        ("max_per_album", "--max-per-album"),
        ("year_confidence_min", "--year-confidence-min"),
        ("manual_year_alpha", "--manual-year-alpha"),
        ("manual_year_rounding", "--manual-year-rounding"),
        ("manual_year_min_k", "--manual-year-min-k"),
    ]
    for key, flag in mappings:
        if key in deck_cfg and not has_flag(passthrough, flag):
            extra.extend([flag, str(deck_cfg[key])])

    return extra


def extra_args_for_stage(
    command: str,
    expansion: str,
    cfg: Dict[str, Any],
    passthrough: Sequence[str],
    stage_args: Sequence[str] | None = None,
) -> List[str]:
    extra: List[str] = []
    if stage_args:
        extra.extend(list(stage_args))

    if command in EXPANSION_AWARE and not has_flag(passthrough, "--expansion"):
        extra.extend(["--expansion", expansion])

    if command in {"export", "instances"}:
        if (
            not has_flag(extra, "--all")
            and not has_flag(extra, "--owner")
            and not has_flag(passthrough, "--all")
            and not has_flag(passthrough, "--owner")
        ):
            extra.append("--all")

    if command == "deck":
        extra.extend(deck_flags_from_config(cfg, passthrough))

    if command in CARDS_STAGES and not has_flag(passthrough, "--deck"):
        extra.extend(["--deck", f"pipeline/data/processed/deck_{expansion}.csv"])

    return extra


def build_command(
    command: str,
    expansion: str,
    cfg: Dict[str, Any],
    passthrough: Sequence[str],
    stage_args: Sequence[str] | None = None,
) -> List[str]:
    script = SCRIPT_PATHS[command]
    cmd = [sys.executable, str(script)]
    cmd.extend(extra_args_for_stage(command, expansion, cfg, passthrough, stage_args=stage_args))
    cmd.extend(list(passthrough))
    return cmd


def run_stage(
    command: str,
    expansion: str,
    cfg: Dict[str, Any],
    passthrough: Sequence[str],
    dry_run: bool,
    stage_args: Sequence[str] | None = None,
) -> int:
    script = SCRIPT_PATHS[command]
    if not script.exists():
        print(f"[run.py] warning: missing stage script, skipping: {script}")
        return 0

    cmd = build_command(command, expansion, cfg, passthrough, stage_args=stage_args)
    print(subprocess.list2cmdline(cmd))

    if dry_run:
        return 0

    child_env = dict(os.environ)
    child_env.setdefault("PYTHONIOENCODING", "utf-8")
    completed = subprocess.run(cmd, cwd=str(ROOT), env=child_env)
    return int(completed.returncode)


def manual_queue_path(expansion: str) -> Path:
    return ROOT / "pipeline" / "manual" / f"manual_year_queue_{expansion}.csv"


def summarize_manual_queue(expansion: str) -> Tuple[bool, Dict[str, Any]]:
    queue_path = manual_queue_path(expansion)
    stats: Dict[str, Any] = {
        "queue_rows": 0,
        "owners_summary": "",
    }

    if not queue_path.exists():
        return False, stats

    owners_counter: Counter[int] = Counter()
    has_owners_count_col = False

    try:
        with queue_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            has_owners_count_col = bool(reader.fieldnames and "owners_count" in reader.fieldnames)
            for row in reader:
                stats["queue_rows"] += 1
                if has_owners_count_col:
                    raw_val = str(row.get("owners_count", "")).strip()
                    try:
                        owners_counter[int(float(raw_val))] += 1
                    except Exception:
                        continue
    except Exception as exc:
        print(f"[run.py] warning: cannot read queue file {queue_path}: {exc}", file=sys.stderr)
        return False, stats

    if has_owners_count_col and owners_counter:
        bits = [f"{owner_count}:{owners_counter[owner_count]}" for owner_count in sorted(owners_counter.keys())]
        stats["owners_summary"] = ", ".join(bits)

    return int(stats["queue_rows"]) > 0, stats


def build_manual_paths(expansion: str) -> Dict[str, str]:
    return {
        "queue": f"pipeline/manual/manual_year_queue_{expansion}.csv",
        "overrides": "pipeline/manual/manual_year_overrides.csv",
        "merges": "pipeline/manual/manual_merges.csv",
        "collapse": f"pipeline/reports/collapse_{expansion}.csv",
        "deck_qc": f"pipeline/reports/deck_qc_{expansion}.csv",
        "deck_build": f"pipeline/reports/deck_build_{expansion}/",
    }


def print_manual_action_banner(expansion: str, paths: Dict[str, str], stats: Dict[str, Any]) -> None:
    use_color = False
    if sys.stdout.isatty():
        try:
            import colorama  # type: ignore

            colorama.just_fix_windows_console()
            use_color = True
        except Exception:
            use_color = bool(os.environ.get("TERM"))

    c_title = "\033[1;97;41m" if use_color else ""
    c_warn = "\033[1;33m" if use_color else ""
    c_reset = "\033[0m" if use_color else ""

    sep = "=" * 92
    rows = int(stats.get("queue_rows", 0))
    owners_summary = str(stats.get("owners_summary", "")).strip()

    print()
    print(sep)
    print(f"{c_title} ACCION MANUAL REQUERIDA (AÑOS) {c_reset}")
    print(sep)
    print(f"{c_warn}Filas pendientes en cola manual:{c_reset} {rows}")
    if owners_summary:
        print(f"{c_warn}Resumen owners_count (owners_count:filas):{c_reset} {owners_summary}")
    print("-" * 92)
    print("A) " + paths["queue"])
    print("   NO editar a mano; es la cola/todo de pendientes. Úsala para identificar canonical_id a corregir.")
    print("B) " + paths["overrides"])
    print(
        "   SI editar: añade/edita filas para fijar el año de una canción. "
        "Toca SOLO canonical_id y campos year del header existente. "
        "NO cambies nombres/orden de headers."
    )
    print("C) " + paths["merges"])
    print(
        "   SI editar SOLO si el colapso ha elegido mal: define merges/keep-drop según el header "
        "del CSV; después hay que rerun desde deck o canonicalize según proceda."
    )
    print("D) " + paths["collapse"])
    print("   NO editar; es diagnóstico para ver qué se colapsó y por qué.")
    print("E) " + paths["deck_qc"] + "  y  " + paths["deck_build"])
    print("   NO editar; QC para validar el mazo.")
    print("-" * 92)
    print("Siguientes comandos:")
    print(f"1) Tras editar overrides: python pipeline/run.py years --expansion {expansion}")
    print(f"2) Después: python pipeline/run.py deck --expansion {expansion}")
    print(
        "3) Luego PDFs: "
        f"python pipeline/run.py all --expansion {expansion} --with-cards "
        f"(o python pipeline/run.py cards_sheets --expansion {expansion}; opcional cards_preview)"
    )
    print(sep)
    print()


def run_stages(
    stages: Sequence[str],
    expansion: str,
    cfg: Dict[str, Any],
    dry_run: bool,
    stage_passthrough: Dict[str, Sequence[str]] | None = None,
    stage_args: Dict[str, Sequence[str]] | None = None,
) -> int:
    for stage in stages:
        passthrough_for_stage: Sequence[str] = []
        if stage_passthrough and stage in stage_passthrough:
            passthrough_for_stage = stage_passthrough[stage]

        args_for_stage: Sequence[str] = []
        if stage_args and stage in stage_args:
            args_for_stage = stage_args[stage]

        rc = run_stage(
            command=stage,
            expansion=expansion,
            cfg=cfg,
            passthrough=passthrough_for_stage,
            dry_run=dry_run,
            stage_args=args_for_stage,
        )
        if rc != 0:
            return rc
    return 0


def card_stage_args_from_namespace(args: argparse.Namespace) -> List[str]:
    extra: List[str] = []
    qr_mm = getattr(args, "qr_mm", None)
    if qr_mm is not None:
        extra.extend(["--qr-mm", str(qr_mm)])
    gradient_mode = getattr(args, "gradient_mode", None)
    if gradient_mode:
        extra.extend(["--gradient-mode", str(gradient_mode)])
    return extra


def stage_args_for_single_command(args: argparse.Namespace) -> Dict[str, Sequence[str]]:
    out: Dict[str, Sequence[str]] = {}
    if args.command == "cards_sheets":
        card_args = card_stage_args_from_namespace(args)
        if card_args:
            out["cards_sheets"] = card_args
        return out
    if args.command not in {"export", "instances"}:
        return out

    stage = args.command
    owner = getattr(args, "owner", None)
    all_owners = bool(getattr(args, "all_owners", False))
    if owner:
        out[stage] = ["--owner", str(owner)]
    elif all_owners:
        out[stage] = ["--all"]
    return out


def stage_args_for_all_command(args: argparse.Namespace) -> Dict[str, Sequence[str]]:
    out: Dict[str, Sequence[str]] = {}
    owner = getattr(args, "owner", None)
    for stage in ("export", "instances"):
        if owner:
            out[stage] = ["--owner", str(owner)]
        else:
            out[stage] = ["--all"]
    card_args = card_stage_args_from_namespace(args)
    if card_args:
        out["cards_sheets"] = card_args
    return out


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

    parser = argparse.ArgumentParser(description="Stable wrapper for pipeline stage scripts.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sub_help = {
        "export": "Run Spotify export parser.",
        "instances": "Build instances CSVs.",
        "canonicalize": "Build linked instances and canonical registry.",
        "years": "Enrich canonical songs with year metadata.",
        "deck": "Build deck CSV/JSON.",
        "cards_preview": "Render one card preview PDF.",
        "cards_sheets": "Render print sheets PDFs.",
    }

    for name in SCRIPT_PATHS:
        stage_parser = subparsers.add_parser(name, parents=[common], help=sub_help[name])
        if name in {"export", "instances"}:
            owner_group = stage_parser.add_mutually_exclusive_group()
            owner_group.add_argument(
                "--owner",
                default=None,
                help="Owner override (solo para esta etapa).",
            )
            owner_group.add_argument(
                "--all",
                dest="all_owners",
                action="store_true",
                help="Procesa todos los owners en esta etapa.",
            )
        if name == "cards_sheets":
            stage_parser.add_argument(
                "--qr-mm",
                type=float,
                default=None,
                help="Pasa --qr-mm a cards_sheets.",
            )
            stage_parser.add_argument(
                "--gradient-mode",
                choices=["png", "pil"],
                default=None,
                help="Modo de fondo para reverso de cartas (png por defecto en renderer).",
            )

    all_parser = subparsers.add_parser("all", parents=[common], help="Run core stages in order.")
    all_parser.add_argument(
        "--owner",
        default=None,
        help="Owner override SOLO para export e instances dentro de all.",
    )
    all_parser.add_argument(
        "--with-cards",
        action="store_true",
        help="Also run cards_sheets if script exists (cards_preview is explicit only).",
    )
    all_parser.add_argument(
        "--qr-mm",
        type=float,
        default=None,
        help="Pasa --qr-mm a cards_sheets cuando se usa --with-cards.",
    )
    all_parser.add_argument(
        "--gradient-mode",
        choices=["png", "pil"],
        default=None,
        help="Pasa --gradient-mode a cards_sheets cuando se usa --with-cards.",
    )

    return parser


def stages_for_command(args: argparse.Namespace) -> List[str]:
    return [args.command]


def main() -> int:
    parser = build_parser()
    args_no_passthrough, passthrough = split_passthrough(sys.argv[1:])
    args = parser.parse_args(args_no_passthrough)

    expansion, config_path, cfg = resolve_runtime_config(args.expansion, args.config)

    if args.config and not config_path.exists():
        parser.error(f"--config not found: {config_path}")

    if args.command == "all" and passthrough:
        parser.error("Passthrough with 'all' is not supported. Run stages individually to pass extra args.")

    if args.command != "all":
        stages = stages_for_command(args)
        rc = run_stages(
            stages=stages,
            expansion=expansion,
            cfg=cfg,
            dry_run=bool(args.dry_run),
            stage_passthrough={args.command: passthrough},
            stage_args=stage_args_for_single_command(args),
        )
        if rc != 0:
            return rc
        return 0

    all_stage_args = stage_args_for_all_command(args)

    rc = run_stages(
        stages=PIPELINE_ALL_CORE,
        expansion=expansion,
        cfg=cfg,
        dry_run=bool(args.dry_run),
        stage_args=all_stage_args,
    )
    if rc != 0:
        return rc

    has_pending_manual, queue_stats = summarize_manual_queue(expansion)
    if has_pending_manual:
        print_manual_action_banner(
            expansion=expansion,
            paths=build_manual_paths(expansion),
            stats=queue_stats,
        )
        if bool(getattr(args, "with_cards", False)):
            print("[run.py] cards generation blocked: manual year queue still has pending rows.", file=sys.stderr)
            return 2

    if bool(getattr(args, "with_cards", False)):
        card_stages = [stage for stage in CARDS_STAGES if SCRIPT_PATHS[stage].exists()]
        rc = run_stages(
            stages=card_stages,
            expansion=expansion,
            cfg=cfg,
            dry_run=bool(args.dry_run),
            stage_args=all_stage_args,
        )
        if rc != 0:
            return rc

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
