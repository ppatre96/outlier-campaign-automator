"""
Regenerate image creatives for a project from a saved creative brief.

Skips all data-pull and analysis stages — loads the creative brief JSON
directly and calls Gemini for all 3 A/B/C variants.

Output: data/project_creatives/project_{project_id}_variant_{angle}.png

Usage:
  PYTHONPATH=. python3 scripts/regen_creatives.py --project-id 69cf1a039ed66cc82e0fa8f3
  PYTHONPATH=. python3 scripts/regen_creatives.py \
      --brief data/dry_run_outputs/69cf1a_creative_brief.json \
      --out-dir data/project_creatives
"""
import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from src.gemini_creative import generate_imagen_creative, _build_imagen_prompt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("regen_creatives")

_DEFAULT_BRIEF_DIR = Path("data/dry_run_outputs")
_DEFAULT_OUT_DIR   = Path("data/project_creatives")


def _find_brief(project_id: str) -> Path:
    """Locate a creative brief JSON for the given project_id."""
    # Canonical path: data/dry_run_outputs/{project_id[:6]}_creative_brief.json
    slug = project_id[:6]
    candidates = [
        _DEFAULT_BRIEF_DIR / f"{slug}_creative_brief.json",
        _DEFAULT_BRIEF_DIR / f"{project_id}_creative_brief.json",
        Path("data") / f"{project_id}_creative_brief.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        f"No creative brief found for project_id={project_id}. "
        f"Looked in: {[str(c) for c in candidates]}"
    )


def regen(
    project_id: str | None = None,
    brief_path: Path | None = None,
    out_dir: Path = _DEFAULT_OUT_DIR,
    angles: list[str] | None = None,
) -> dict[str, Path]:
    """
    Regenerate creatives from a saved brief.

    Args:
        project_id:  Project ID string (used to locate brief + name outputs).
        brief_path:  Direct path to creative brief JSON (overrides project_id lookup).
        out_dir:     Directory to write the output PNGs.
        angles:      List of angles to generate, e.g. ["A", "B", "C"]. Defaults to all.

    Returns:
        Dict mapping angle label to saved Path, e.g. {"A": Path(...), "B": ..., "C": ...}
    """
    # ── Load brief ─────────────────────────────────────────────────────────────
    if brief_path is None:
        if project_id is None:
            raise ValueError("Provide either project_id or brief_path")
        brief_path = _find_brief(project_id)

    log.info("Loading creative brief from %s", brief_path)
    brief = json.loads(brief_path.read_text())

    proj_id = brief.get("project_id") or project_id or "unknown"
    variants = brief.get("variants", [])
    if not variants:
        raise ValueError(f"No variants found in brief: {brief_path}")

    log.info("Loaded brief — project_id=%s, %d variants", proj_id, len(variants))

    # ── Print prompt previews before generating ────────────────────────────────
    from src.gemini_creative import _REFERENCE_IMAGE_B64, _OUTLIER_LOGO_SVG
    ref_status = f"inline ({len(_REFERENCE_IMAGE_B64):,} chars b64)" if _REFERENCE_IMAGE_B64 else "NOT FOUND (degraded)"
    logo_status = f"inline ({len(_OUTLIER_LOGO_SVG)} chars SVG)" if _OUTLIER_LOGO_SVG else "NOT FOUND (fallback text)"

    print(f"\nPrompt previews (inline reference materials):")
    print(f"  Finance-Branded PNG : {ref_status}")
    print(f"  Outlier logo SVG    : {logo_status}")
    print("=" * 70)
    for v in variants:
        angle         = v.get("angle", "?")
        photo_subject = v.get("photo_subject", "")
        prompt_text, _ = _build_imagen_prompt(photo_subject, angle)
        print(f"\n  Variant {angle} ({v.get('angleLabel', '')}):")
        print(f"  photo_subject : {photo_subject}")
        print(f"  headline      : {v.get('headline', '')}")
        print(f"  subheadline   : {v.get('subheadline', '')}")
        print(f"  Prompt (first 300 chars):")
        print(f"    {prompt_text[:300].replace(chr(10), chr(10) + '    ')}")
    print("=" * 70)

    # ── Create output directory ────────────────────────────────────────────────
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Generate each variant ─────────────────────────────────────────────────
    angles_to_run = set(angles) if angles else {"A", "B", "C"}
    results: dict[str, Path] = {}

    for v in variants:
        angle = v.get("angle", "")
        if angle not in angles_to_run:
            log.info("Skipping angle %s (not in requested angles)", angle)
            continue

        photo_subject = v.get("photo_subject", "")
        headline      = v.get("headline", "")
        subheadline   = v.get("subheadline", "")

        print(f"\n  Generating Angle {angle}: {headline!r}")
        print(f"    photo_subject : {photo_subject}")
        print(f"    subheadline   : {subheadline}")

        try:
            tmp_path = generate_imagen_creative(
                variant=v,
                photo_subject=photo_subject,
            )
            out_path = out_dir / f"project_{proj_id}_variant_{angle}.png"
            shutil.copy2(tmp_path, out_path)
            tmp_path.unlink(missing_ok=True)
            size_kb = out_path.stat().st_size // 1024
            print(f"    Saved: {out_path} ({size_kb} KB)")
            results[angle] = out_path
        except Exception as exc:
            log.error("Image gen failed for angle %s: %s", angle, exc)
            print(f"    FAILED: {exc}")

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Regenerate Gemini image creatives from a saved creative brief"
    )
    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument(
        "--project-id",
        help="Project ID — used to locate brief in data/dry_run_outputs/{id[:6]}_creative_brief.json",
    )
    id_group.add_argument(
        "--brief",
        help="Direct path to creative brief JSON file",
    )
    parser.add_argument(
        "--out-dir",
        default=str(_DEFAULT_OUT_DIR),
        help=f"Output directory (default: {_DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--angles",
        nargs="+",
        choices=["A", "B", "C"],
        default=None,
        help="Which angles to generate (default: all A B C)",
    )
    args = parser.parse_args()

    brief_path = Path(args.brief) if args.brief else None

    results = regen(
        project_id=args.project_id,
        brief_path=brief_path,
        out_dir=Path(args.out_dir),
        angles=args.angles,
    )

    print(f"\nRegeneration complete — {len(results)} creative(s) saved:")
    for angle, path in sorted(results.items()):
        print(f"  Angle {angle}: {path}")

    if not results:
        print("  No creatives generated — check logs above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
