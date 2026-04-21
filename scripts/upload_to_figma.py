"""
Upload dry-run PNG creatives to Figma via the Plugin API (use_figma MCP).

This script prints the base64 + frame names that Claude needs to call use_figma.
Claude reads this output and executes the Figma upload in one or more MCP calls.

Usage:
  PYTHONPATH=. python3 scripts/upload_to_figma.py [--project-id <id>] [--dir data/dry_run_outputs]

When --project-id is given, only files whose name contains that id slug are included.
Without it, the most recent dry-run PNG per angle is uploaded.

After running this, tell Claude:
  "Upload the listed creatives to Figma using the use_figma MCP tool."
"""

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

# PYTHONPATH=. required
from src.figma_upload import prepare_for_figma, list_dry_run_outputs, parse_dry_run_filename, FIGMA_FILE_KEY


def run(project_id: str | None = None, output_dir: str = "data/dry_run_outputs") -> None:
    files = list_dry_run_outputs(output_dir)
    if not files:
        print(f"No dry-run PNGs found in {output_dir}/")
        sys.exit(1)

    # Filter by project_id if given
    if project_id:
        # project_id maps to stg_id slug in filename; just match on substring
        short = project_id[:13]  # STG slug uses first 13 digits of timestamp
        files = [f for f in files if short in f.name or project_id in f.name]
        if not files:
            print(f"No PNGs found matching project_id={project_id}")
            sys.exit(1)

    print(f"Figma file: https://www.figma.com/design/{FIGMA_FILE_KEY}/")
    print(f"Found {len(files)} creative(s) to upload:\n")

    for png_path in files:
        slug, angle = parse_dry_run_filename(png_path.name)
        if not angle:
            print(f"  SKIP (unrecognised filename): {png_path.name}")
            continue

        # Frame name uses the STG slug as the project identifier
        frame_project_id = project_id or slug or "unknown"

        try:
            info = prepare_for_figma(png_path, project_id=frame_project_id, angle=angle)
        except Exception as exc:
            print(f"  ERROR compressing {png_path.name}: {exc}")
            continue

        print(f"  File      : {png_path}")
        print(f"  Frame name: {info['frame_name']}")
        print(f"  Size      : {info['bytes']:,} bytes compressed ({len(info['b64']):,} base64 chars)")
        print(f"  B64 (first 80 chars): {info['b64'][:80]}...")
        print()

    print("Next step: ask Claude to run use_figma MCP to upload these creatives to Figma.")
    print("Or: Claude Code will read this output and upload automatically when you say 'upload to Figma'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List dry-run creatives ready for Figma upload")
    parser.add_argument("--project-id", default=None, help="Filter by project_id")
    parser.add_argument("--dir", default="data/dry_run_outputs", help="Directory with PNG files")
    args = parser.parse_args()
    run(project_id=args.project_id, output_dir=args.dir)
