"""
Regenerate creatives for a project with freshly generated copy.

Unlike regen_creatives.py (which reuses cached copy), this script:
  1. Loads the saved creative brief for cohort context (cohort_name + pass_rate)
  2. Reconstructs a minimal cohort object and generates copy via LiteLLM (or
     Gemini text API as fallback when the internal LiteLLM proxy is unreachable)
  3. Validates all copy against the tighter limits (≤6w/≤40c hl, ≤7w/≤48c sh)
  4. Calls generate_imagen_creative() for all 3 A/B/C angles
  5. Saves to data/project_creatives/

Usage:
  DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
  PYTHONPATH=/Users/pranavpatre/outlier-campaign-agent \\
    python3 scripts/regen_with_fresh_copy.py --project-id 69cf1a039ed66cc82e0fa8f3
"""
import argparse
import json
import logging
import os
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

# Ensure Homebrew libs (cairosvg/libcairo) are found before any imports
os.environ.setdefault("DYLD_FALLBACK_LIBRARY_PATH", "/opt/homebrew/lib")

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from src.figma_creative import (
    _build_copy_prompt, _extract_json, _validate_copy_limits,
)
from src.gemini_creative import generate_imagen_creative, _rasterize_outlier_logo, _REFERENCE_IMAGE_B64

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("regen_copy")

_BRIEF_DIR  = Path("/Users/pranavpatre/outlier-campaign-agent/data/dry_run_outputs")
_OUT_DIR    = Path("/Users/pranavpatre/outlier-campaign-agent/data/project_creatives")

# ── LLM copy generation with fallback ────────────────────────────────────────

def _generate_copy_via_gemini(prompt: str, gemini_api_key: str) -> str:
    """Call Google Gemini text API directly for copy generation."""
    import requests as req_lib
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models"
        f"/gemini-2.5-flash:generateContent?key={gemini_api_key}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "maxOutputTokens": 4096,
            "temperature": 0.7,
            "responseMimeType": "application/json",  # force pure JSON output — no markdown fences
        },
    }
    resp = req_lib.post(url, json=payload, timeout=90)
    if resp.status_code != 200:
        raise RuntimeError(f"Gemini text API error {resp.status_code}: {resp.text[:300]}")
    parts = (
        resp.json()
        .get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [])
    )
    for part in parts:
        if "text" in part:
            return part["text"]
    raise RuntimeError("Gemini text API returned no text in response")


def _build_variants(cohort, gemini_api_key: str) -> list[dict]:
    """
    Generate 3 copy variants. Tries LiteLLM first; falls back to direct Gemini text API.
    Includes the same retry-on-violation logic as build_copy_variants().
    """
    import config
    from src.linkedin_urn import _col_to_human
    from src.analysis import _feature_to_facet

    signals = []
    for feat, _ in cohort.rules:
        human = _col_to_human(feat)
        facet = _feature_to_facet(feat)
        signals.append(f"{facet}: {human}")

    # Load competitor intel if available
    competitor_context = ""
    intel_path = Path("/Users/pranavpatre/outlier-campaign-agent/data/competitor_intel/latest.json")
    if intel_path.exists():
        try:
            intel_data = json.loads(intel_path.read_text())
            ideas = intel_data.get("experiment_ideas", [])
            if ideas:
                competitor_context = "\n\nCompetitor experiment ideas to consider:\n" + "\n".join(f"- {i}" for i in ideas[:3])
                log.info("Loaded %d competitor experiment ideas for copy gen", len(ideas))
        except Exception as exc:
            log.warning("Failed to load competitor intel: %s", exc)

    base_prompt = _build_copy_prompt(cohort.name, signals, {})
    if competitor_context:
        base_prompt += competitor_context

    log.info("Copy gen — cohort=%s  signals=%d", cohort.name[:50], len(signals))

    # Determine which backend to use
    use_litellm = False
    try:
        import socket
        socket.setdefaulttimeout(3)
        host = "litellm-proxy.ml-serving-internal.scale.com"
        socket.getaddrinfo(host, 443)
        use_litellm = True
        log.info("LiteLLM proxy DNS resolves — using LiteLLM for copy gen")
    except Exception:
        log.info("LiteLLM proxy unreachable — falling back to direct Gemini text API")
    finally:
        socket.setdefaulttimeout(None)

    variants: list[dict] = []
    last_violations: list[str] = []

    for attempt in range(3):
        prompt = base_prompt
        if attempt > 0 and last_violations:
            retry_note = (
                "\n\nRETRY — your previous output violated the hard limits:\n"
                + "\n".join(f"- {v}" for v in last_violations)
                + "\nREWRITE so every headline is ≤6 words AND ≤40 chars, every subheadline is "
                  "≤7 words AND ≤48 chars. No exceptions."
            )
            prompt = base_prompt + retry_note

        try:
            if use_litellm:
                from openai import OpenAI
                client = OpenAI(
                    base_url=config.LITELLM_BASE_URL,
                    api_key=config.LITELLM_API_KEY,
                )
                resp = client.chat.completions.create(
                    model=config.LITELLM_MODEL,
                    max_tokens=2048,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw = resp.choices[0].message.content.strip()
            else:
                raw = _generate_copy_via_gemini(prompt, gemini_api_key)
        except Exception as exc:
            log.error("Copy gen attempt %d failed: %s", attempt + 1, exc)
            continue

        try:
            parsed = _extract_json(raw)
            new_variants = parsed.get("variants", [])
        except Exception as exc:
            log.error("JSON parse failed (attempt %d): %s\n%s", attempt + 1, exc, raw[:500])
            # Don't reset variants — keep the best we've seen so far
            continue

        if new_variants:
            variants = new_variants  # always keep the latest parseable result
        new_violations = _validate_copy_limits(variants)
        if not new_violations:
            log.info("Copy gen succeeded on attempt %d", attempt + 1)
            last_violations = []
            break
        last_violations = new_violations
        log.warning("Copy limits violated (attempt %d): %s", attempt + 1, last_violations)

    if last_violations:
        log.warning("Proceeding with violations after 3 attempts: %s", last_violations)

    log.info("Generated %d copy variants for '%s'", len(variants), cohort.name)
    return variants


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_brief(project_id: str) -> dict:
    slug = project_id[:6]
    candidates = [
        _BRIEF_DIR / f"{slug}_creative_brief.json",
        _BRIEF_DIR / f"{project_id}_creative_brief.json",
    ]
    for p in candidates:
        if p.exists():
            log.info("Brief loaded from %s", p)
            return json.loads(p.read_text())
    raise FileNotFoundError(f"No brief for project_id={project_id}")


def _cohort_from_brief(brief: dict) -> SimpleNamespace:
    cohort_name = brief.get("cohort_name", "unknown")
    rules = [(cohort_name, 1)]
    if "rules" in brief:
        rules = [(r["feature"], r["value"]) for r in brief["rules"]]
    c = SimpleNamespace()
    c.name      = cohort_name
    c.rules     = rules
    c.pass_rate = brief.get("pass_rate", 0.0)
    return c


def _word_count(text: str) -> int:
    return len(text.replace("\n", " ").split())


def _char_count(text: str) -> int:
    return len(text.replace("\n", ""))


# ── Main ───────────────────────────────────────────────────────────────────────

def run(project_id: str) -> None:
    import config
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    gemini_api_key = config.GEMINI_API_KEY
    if not gemini_api_key:
        print("ERROR: GEMINI_API_KEY is not set — cannot fall back to direct Gemini API")
        sys.exit(1)

    # ── Pre-flight: confirm logo SVG and reference image ───────────────────────
    logo_img = _rasterize_outlier_logo(target_width=400)
    if logo_img is not None:
        print(f"  Logo SVG    : FOUND — rasterized to {logo_img.width}x{logo_img.height}px, tinted #3D1A00")
        logo_source = "Outlier SVG rasterized via cairosvg"
    else:
        print("  Logo SVG    : NOT FOUND — will fall back to text wordmark")
        logo_source = "FALLBACK TEXT (outlier logo.svg not found)"

    ref_status = f"{len(_REFERENCE_IMAGE_B64):,} chars b64 (inline)" if _REFERENCE_IMAGE_B64 else "NOT FOUND"
    print(f"  Ref image   : {ref_status}")
    print()

    # ── Load brief ─────────────────────────────────────────────────────────────
    brief  = _load_brief(project_id)
    cohort = _cohort_from_brief(brief)
    print(f"  Project     : {project_id}")
    print(f"  Config      : {brief.get('config_name', 'unknown')}")
    print(f"  Cohort      : {cohort.name}  (pass_rate={cohort.pass_rate:.1f}%)")
    print(f"  Rules       : {cohort.rules[:3]}")
    print()

    # ── Generate fresh copy ────────────────────────────────────────────────────
    print("  Generating fresh copy variants (≤6w/≤40c headline, ≤7w/≤48c subheadline) ...")
    variants = _build_variants(cohort, gemini_api_key)
    if not variants:
        print("  FAILED: copy generation returned 0 variants")
        sys.exit(1)

    # ── Validate and report ────────────────────────────────────────────────────
    violations = _validate_copy_limits(variants)
    print(f"\n  Copy generated ({len(variants)} variants):")
    print("  " + "=" * 66)
    for v in variants:
        angle = v.get("angle", "?")
        hl    = v.get("headline", "").replace("\n", " ")
        sh    = v.get("subheadline", "").replace("\n", " ")
        ps    = v.get("photo_subject", "")
        hl_w  = _word_count(hl)
        hl_c  = _char_count(hl)
        sh_w  = _word_count(sh)
        sh_c  = _char_count(sh)
        hl_ok = "OK" if hl_w <= 6 and hl_c <= 40 else f"VIOLATION ({hl_w}w/{hl_c}c)"
        sh_ok = "OK" if sh_w <= 7 and sh_c <= 48 else f"VIOLATION ({sh_w}w/{sh_c}c)"
        print(f"\n  Angle {angle}:")
        print(f"    headline    [{hl_ok}] ({hl_w}w/{hl_c}c) : {hl!r}")
        print(f"    subheadline [{sh_ok}] ({sh_w}w/{sh_c}c) : {sh!r}")
        print(f"    photo_subj  : {ps}")

    print()
    if violations:
        print(f"  WARNING: {len(violations)} copy limit violation(s) remain after retry:")
        for viol in violations:
            print(f"    - {viol}")
        print("  Proceeding (compose_ad will wrap within safe zones).")
    else:
        print("  All copy within limits.")

    # ── Generate images ────────────────────────────────────────────────────────
    print("\n  Generating images (3 angles via Gemini) ...")
    results: dict[str, Path] = {}
    for v in variants:
        angle         = v.get("angle", "A")
        headline      = v.get("headline", "").replace("\n", " ")
        subheadline   = v.get("subheadline", "").replace("\n", " ")
        photo_subject = v.get("photo_subject", "")

        print(f"\n  ── Angle {angle} ──")
        print(f"     headline    : {headline!r}")
        print(f"     subheadline : {subheadline!r}")
        print(f"     photo_subj  : {photo_subject}")

        v_flat = dict(v)
        v_flat["headline"]    = headline
        v_flat["subheadline"] = subheadline

        try:
            tmp_path = generate_imagen_creative(
                variant=v_flat,
                photo_subject=photo_subject,
                gemini_api_key=gemini_api_key,
            )
            out_path = _OUT_DIR / f"project_{project_id}_variant_{angle}.png"
            shutil.copy2(tmp_path, out_path)
            tmp_path.unlink(missing_ok=True)
            size_kb = out_path.stat().st_size // 1024
            print(f"     Saved: {out_path} ({size_kb} KB)")
            results[angle] = out_path
        except Exception as exc:
            log.error("Image gen failed for angle %s: %s", angle, exc)
            print(f"     FAILED: {exc}")

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"  DONE — {len(results)}/3 creatives saved to {_OUT_DIR}")
    for angle, path in sorted(results.items()):
        v    = next((x for x in variants if x.get("angle") == angle), {})
        hl   = v.get("headline", "").replace("\n", " ")
        sh   = v.get("subheadline", "").replace("\n", " ")
        hl_w = _word_count(hl)
        sh_w = _word_count(sh)
        hl_c = _char_count(hl)
        sh_c = _char_count(sh)
        print(f"\n  Angle {angle}: {path}")
        print(f"    headline    ({hl_w}w/{hl_c}c) : {hl!r}")
        print(f"    subheadline ({sh_w}w/{sh_c}c) : {sh!r}")

    print(f"\n  Logo source : {logo_source}")

    if len(results) < 3:
        print(f"\n  WARNING: Only {len(results)} of 3 angles completed — check logs above.")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Regenerate Gemini creatives with freshly generated copy"
    )
    parser.add_argument("--project-id", required=True, help="Project ID (24-char hex)")
    args = parser.parse_args()
    run(args.project_id)


if __name__ == "__main__":
    main()
