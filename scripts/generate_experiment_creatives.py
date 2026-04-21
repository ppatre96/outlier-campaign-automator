"""
Generate challenger ad creatives for the 3 pending experiments in data/experiment_queue.json.

Briefs were produced by ad-creative-brief-generator agents on 2026-04-16.
This script calls generate_midjourney_creative() for each and saves composed PNGs
to data/experiment_outputs/.

Run:
  PYTHONPATH=/Users/pranavpatre/outlier-campaign-agent \
  python scripts/generate_experiment_creatives.py
"""
import json
import shutil
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import config
from src.midjourney_creative import generate_midjourney_creative
from src.gdrive import upload_creative

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "experiment_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

QUEUE_PATH = Path(__file__).parent.parent / "data" / "experiment_queue.json"

# ── Briefs from ad-creative-brief-generator agents (2026-04-16) ───────────────

# photo_subject format (match Outlier reference template):
#   "[gender] [ethnicity] [profession], at home [activity]"
# SHORT — do not describe the full scene. _build_imagen_prompt() adds:
#   plants/nature room, natural light, 85mm prime lens, expression, style suffix.

EXPERIMENTS = [
    {
        "experiment_id": "exp_20260416_1370626596",
        "campaign_id": 559000496,
        "description": "deepanshu en-us — workspace + expert identity (Angle A)",
        "variant": {
            "angle": "A",
            "headline": "Your code review eye has AI value.",
            "subheadline": "Put your dev expertise to work. Earn payment from home.",
            "cta": "Apply Now",
        },
        "photo_subject": "male South Asian software developer, at home working at a laptop with code on the screen",
        "bottom_text": "Earn $25–$50 USD per hour. Fully remote.",
        "output_filename": "exp_deepanshu_enUS_angleA.png",
    },
    {
        "experiment_id": "exp_20260416_1141921754",
        "campaign_id": 535014976,
        "description": "oleksandrr uk-UA — workspace + weekly rate framing (Angle B)",
        "variant": {
            "angle": "B",
            "headline": "Thousands of developers paid weekly in USD",
            "subheadline": "Earn $500–$2,000 weekly from your home desk.",
            "cta": "Start Earning",
        },
        "photo_subject": "male Eastern European software developer, at home typing on a laptop with code visible on screen",
        "bottom_text": "Earn $500+ USD per week. Fully remote.",
        "output_filename": "exp_oleksandrr_ukUA_angleB.png",
    },
    {
        "experiment_id": "exp_20260416_1144706884",
        "campaign_id": 535614956,
        "description": "mariiac uk-UA — workspace + expert identity (Angle A)",
        "variant": {
            "angle": "A",
            "headline": "Your Ukrainian expertise is what AI needs",
            "subheadline": "Apply it. Get paid. Work from home.",
            "cta": "Apply Now",
        },
        "photo_subject": "female Eastern European language professional, at home working on a laptop reviewing documents",
        "bottom_text": "Earn $25–$50 USD per hour. Fully remote.",
        "output_filename": "exp_mariiac_ukUA_angleA.png",
    },
]


def run():
    results = []

    for exp in EXPERIMENTS:
        exp_id = exp["experiment_id"]
        print(f"\n{'='*60}")
        print(f"Generating: {exp['description']}")
        print(f"Experiment: {exp_id}")
        print(f"Headline:   {exp['variant']['headline']}")
        print(f"Sub:        {exp['variant']['subheadline']}")

        try:
            tmp_path = generate_midjourney_creative(
                variant=exp["variant"],
                photo_subject=exp["photo_subject"],
            )
            # Copy from temp to persistent output dir
            out_path = OUTPUT_DIR / exp["output_filename"]
            shutil.copy2(tmp_path, out_path)
            tmp_path.unlink(missing_ok=True)

            size_kb = out_path.stat().st_size // 1024
            print(f"Saved:      {out_path} ({size_kb} KB)")

            # Upload to Google Drive (only when GDRIVE_ENABLED=true in .env)
            drive_url = None
            if config.GDRIVE_ENABLED:
                try:
                    drive_url = upload_creative(out_path)
                    print(f"Drive:      {drive_url}")
                except Exception as exc:
                    print(f"Drive upload failed: {exc}")

            results.append({
                "experiment_id": exp_id,
                "status": "success",
                "output_path": str(out_path),
                "size_kb": size_kb,
                "drive_url": drive_url,
            })

        except Exception as exc:
            print(f"ERROR: {exc}")
            results.append({
                "experiment_id": exp_id,
                "status": "error",
                "error": str(exc),
            })

    # ── Update experiment_queue.json ──────────────────────────────────────────
    queue = json.loads(QUEUE_PATH.read_text())
    success_ids = {r["experiment_id"] for r in results if r["status"] == "success"}

    for entry in queue:
        if entry["id"] in success_ids:
            entry["status"] = "dispatched"
            entry["implemented_at"] = "2026-04-16"
            matching = next(r for r in results if r["experiment_id"] == entry["id"])
            if "challenger_creative" not in entry or entry["challenger_creative"] is None:
                entry["challenger_creative"] = {}
            entry["challenger_creative"]["local_path"] = matching["output_path"]
            entry["challenger_creative"]["size_kb"] = matching["size_kb"]

    QUEUE_PATH.write_text(json.dumps(queue, indent=2))
    print(f"\n{'='*60}")
    print(f"Queue updated: {sum(1 for r in results if r['status'] == 'success')}/3 experiments dispatched")
    print(f"Results saved to: {OUTPUT_DIR}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\nSummary:")
    for r in results:
        status_sym = "✓" if r["status"] == "success" else "✗"
        if r["status"] == "success":
            drive_str = f" | {r['drive_url']}" if r.get("drive_url") else ""
            print(f"  {status_sym} {r['experiment_id']} → {r['output_path']} ({r['size_kb']} KB){drive_str}")
        else:
            print(f"  {status_sym} {r['experiment_id']} → ERROR: {r['error']}")


if __name__ == "__main__":
    run()
