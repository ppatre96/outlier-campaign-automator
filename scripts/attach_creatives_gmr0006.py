"""
One-shot: regenerate Gemini images for all GMR-0006 campaigns that lack creatives
and attach them to the existing LinkedIn campaign URNs.

Usage:
    venv/bin/python scripts/attach_creatives_gmr0006.py
    venv/bin/python scripts/attach_creatives_gmr0006.py --dry-run
"""
import argparse
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import config
from src.linkedin_api import LinkedInClient
from src.gemini_creative import generate_imagen_creative_with_qc
from src.figma_creative import rewrite_variant_copy
from src.campaign_registry import _load, _save, COLUMNS

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _build_variant(rec: dict) -> dict:
    return {
        "angle":         rec.get("angle", "A"),
        "headline":      rec.get("headline", ""),
        "subheadline":   rec.get("subheadline", ""),
        "photo_subject": rec.get("photo_subject", ""),
        "ad_headline":   rec.get("headline", ""),
        "ad_description": rec.get("subheadline", ""),
        "cta":           "Apply Now",
        "tgLabel":       rec.get("cohort_signature", ""),
        "intro_text":    "",
    }


def run(dry_run: bool = False) -> None:
    records = _load()
    to_attach = [r for r in records if not r.get("creative_urn") and r.get("linkedin_campaign_urn")]
    log.info("%d of %d campaigns need creatives", len(to_attach), len(records))

    if not to_attach:
        log.info("All campaigns already have creatives — nothing to do.")
        return

    li_client = None if dry_run else LinkedInClient(token=config.LINKEDIN_TOKEN)

    sheets_ws = None
    if not dry_run:
        try:
            from src.sheets import SheetsClient
            sheets_ws = SheetsClient()._get_or_create_registry_tab()
        except Exception as exc:
            log.warning("Could not connect to Google Sheets (non-fatal): %s", exc)

    urn_col_idx = COLUMNS.index("creative_urn") + 1  # 1-based for gspread

    for i, rec in enumerate(to_attach, 1):
        campaign_urn = rec["linkedin_campaign_urn"]
        angle     = rec.get("angle", "A")
        geo_label = rec.get("geo_cluster_label", "")
        cohort    = rec.get("cohort_signature", "")[:45]
        log.info("[%d/%d] %s | %s | angle=%s", i, len(to_attach), cohort, geo_label, angle)

        variant = _build_variant(rec)

        # Generate image
        png_path = None
        try:
            png_path, qc_report = generate_imagen_creative_with_qc(
                variant=variant,
                copy_rewriter=rewrite_variant_copy,
            )
            verdict = (qc_report or {}).get("verdict", "?")
            violations = (qc_report or {}).get("violations", [])
            log.info("  QC: %s  violations=%d  path=%s", verdict, len(violations), png_path)
        except Exception as exc:
            log.warning("  Image gen failed: %s — skipping", exc)
            continue

        if not png_path or not Path(str(png_path)).exists():
            log.warning("  No valid PNG — skipping")
            continue

        # Save to named output dir regardless of LinkedIn status
        out_dir = ROOT / "data" / "ramp_creatives" / "GMR-0006"
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_cohort = cohort.replace("/", "_").replace(" ", "_").replace("+", "and")[:40]
        dest = out_dir / f"{safe_cohort}_{geo_label.replace('/', '_')}_{angle}.png"
        import shutil
        shutil.copy2(str(png_path), str(dest))
        log.info("  Saved: %s", dest.name)

        if dry_run:
            log.info("  [dry-run] would attach to %s", campaign_urn)
            continue

        # Upload + attach creative
        try:
            image_urn = li_client.upload_image(png_path)
            result = li_client.create_image_ad(
                campaign_urn=campaign_urn,
                image_urn=image_urn,
                headline=variant["headline"],
                description=variant["subheadline"],
                cta_button="APPLY",
            )
        except Exception as exc:
            log.warning("  LinkedIn attach failed: %s — skipping creative attach (image saved at %s)", exc, dest.name)
            continue

        if result.status != "ok":
            log.warning("  create_image_ad returned %s — image saved at %s for manual upload", result.status, dest.name)
            continue

        creative_urn = result.creative_urn
        log.info("  Creative attached: %s", creative_urn)

        # Update registry JSON
        for r in records:
            if r.get("linkedin_campaign_urn") == campaign_urn:
                r["creative_urn"] = creative_urn
                break
        _save(records)

        # Update sheet
        if sheets_ws:
            try:
                all_vals = sheets_ws.get_all_values()
                for row_idx, row in enumerate(all_vals[1:], start=2):
                    if len(row) > 9 and row[9] == campaign_urn:
                        sheets_ws.update_cell(row_idx, urn_col_idx, creative_urn)
                        break
            except Exception as exc:
                log.warning("  Sheet update failed (non-fatal): %s", exc)

        time.sleep(1)

    attached = sum(1 for r in _load() if r.get("creative_urn"))
    log.info("Done. %d/%d campaigns now have creatives.", attached, len(records))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Attach Gemini creatives to GMR-0006 campaigns")
    parser.add_argument("--dry-run", action="store_true", help="Generate images but skip LinkedIn upload")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
