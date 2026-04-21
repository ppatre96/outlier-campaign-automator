"""
figma_upload.py — Prepare dry-run creative metadata for Figma frame creation.

IMPORTANT: figma.createImage() in the Plugin API MCP sandbox only works for
very small images (~100 bytes). Large PNG/JPEG creatives appear black.

The correct approach is to recreate the ad creative as **native Figma elements**
(frame + gradient fills + text nodes) using use_figma MCP. This module provides:
  - Frame naming helpers so Claude knows what to name the frame
  - Metadata extraction from dry-run PNGs (copy, cohort label)
  - The base64 path is kept for reference but is NOT used for Figma upload

Frame naming convention: "<project_id>_<angle>_v<version>"
  e.g. "69cf1a039ed66cc82e0fa8f3_A_v1"

Usage (from Claude Code context):
    from src.figma_upload import get_figma_frame_name, list_dry_run_outputs
    frame_name = get_figma_frame_name(project_id="69cf1a039ed66cc82e0fa8f3", angle="A")
    # Claude then calls use_figma to create the frame with native elements

For bulk listing of dry-run outputs, use scripts/upload_to_figma.py.
"""

import base64
import io
import re
from pathlib import Path

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False


# Figma file where creatives land
FIGMA_FILE_KEY = "j16txqhVXak2TON1w5sdAH"

# Target dimensions and quality — keeps base64 < 40 KB (well within 50 KB code limit)
_TARGET_WIDTH  = 400
_JPEG_QUALITY  = 55


def get_figma_frame_name(project_id: str, angle: str, version: int = 1) -> str:
    """Return the Figma frame name for a creative. Format: '<project_id>_<angle>_v<version>'"""
    return f"{project_id}_{angle}_v{version}"


def prepare_for_figma(
    png_path: str | Path,
    project_id: str,
    angle: str,
    version: int = 1,
) -> dict:
    """
    Return frame metadata for Figma native-element creation.

    NOTE: The base64 field is kept for reference only — do NOT pass it to
    figma.createImage() as large images render black in the Plugin API sandbox.
    Instead, use the frame_name and create native Figma elements (gradient fills
    + text nodes) via use_figma MCP using the copy from the variant dict.

    Returns:
        {
          "b64":        str,   # JPEG base64 (reference only — not for figma.createImage)
          "frame_name": str,   # "<project_id>_<angle>_v<version>"
          "file_key":   str,   # Figma file key
          "bytes":      int,   # compressed size
        }
    """
    if not PIL_AVAILABLE:
        raise RuntimeError("Pillow not installed — run: pip install Pillow")

    png_path = Path(png_path)
    if not png_path.exists():
        raise FileNotFoundError(f"PNG not found: {png_path}")

    img = Image.open(png_path)
    ratio = _TARGET_WIDTH / img.width
    new_h = max(1, int(img.height * ratio))
    img_small = img.resize((_TARGET_WIDTH, new_h), Image.LANCZOS)

    buf = io.BytesIO()
    img_small.convert("RGB").save(buf, format="JPEG", quality=_JPEG_QUALITY)
    raw = buf.getvalue()

    return {
        "b64":        base64.b64encode(raw).decode(),
        "frame_name": get_figma_frame_name(project_id, angle, version),
        "file_key":   FIGMA_FILE_KEY,
        "bytes":      len(raw),
    }


def png_to_base64(png_path: str | Path) -> str:
    """
    Convert a PNG file to base64 data URI for embedding in Figma frames.

    Returns a string with format: "data:image/png;base64,<base64_data>"

    This is used by build_figma_layered_frame_js() to embed the Gemini-generated
    photo as a raster layer in Figma frames.

    Args:
        png_path: Path to the PNG file

    Returns:
        Base64-encoded PNG with data URI prefix

    Raises:
        FileNotFoundError: If PNG file does not exist
    """
    png_path = Path(png_path)
    if not png_path.exists():
        raise FileNotFoundError(f"PNG not found: {png_path}")

    with open(png_path, "rb") as f:
        png_data = f.read()

    b64 = base64.b64encode(png_data).decode()
    return f"data:image/png;base64,{b64}"


def parse_dry_run_filename(filename: str) -> tuple[str | None, str | None]:
    """
    Extract (stg_id_slug, angle) from a dry-run filename like:
      dry_2026042029673_A.png  →  ("2026042029673", "A")
    Returns (None, None) if pattern doesn't match.
    """
    m = re.match(r"dry_(\w+?)_([A-C])\.png$", filename, re.IGNORECASE)
    if not m:
        return None, None
    return m.group(1), m.group(2).upper()


def list_dry_run_outputs(output_dir: str | Path = "data/dry_run_outputs") -> list[Path]:
    """Return all PNG files in the dry-run output directory, newest first."""
    d = Path(output_dir)
    if not d.exists():
        return []
    return sorted(d.glob("dry_*.png"), key=lambda p: p.stat().st_mtime, reverse=True)


def build_figma_layered_frame_js(
    frame_name: str,
    headline: str,
    subheadline: str,
    angle: str,
    photo_base64: str,
    earnings: str = "Earn $25–$50 USD per hour.",
) -> str:
    """
    Build JavaScript to create a fully deconstructed Figma frame with separate editable layers.

    Args:
        frame_name: Target frame name, e.g. "abc123_A_v1"
        headline: Headline text (Inter Bold, white)
        subheadline: Subheadline text (Inter Regular, white)
        angle: "A", "B", or "C" — determines gradient colors
        photo_base64: Gemini image as base64 data (PNG)
        earnings: Earnings text for bottom strip

    Returns:
        JavaScript string for use_figma MCP.

    Creates:
      1. 1200×1200 frame with proper naming
      2. Background image layer (from base64 photo)
      3. Gradient overlay rectangles (vector shapes, angle-specific colors)
      4. Text layers (headline, subheadline, earnings) — all editable
    """
    # Gradient colors per angle
    gradients = {
        "A": ("rgba(173,217,229,0.4)", "rgba(255,181,194,0.45)"),  # light blue + pink
        "B": ("rgba(173,217,229,0.4)", "rgba(255,165,100,0.4)"),   # light blue + orange
        "C": ("rgba(173,217,229,0.4)", "rgba(144,238,144,0.35)"),  # light blue + light green
    }
    grad_a, grad_b = gradients.get(angle, gradients["A"])

    # Escape for safe JS embedding
    fn_safe = frame_name.replace("'", "\\'")
    hl_safe = headline.replace("'", "\\'")
    sub_safe = subheadline.replace("'", "\\'")
    earn_safe = earnings.replace("'", "\\'")

    js = f"""(async () => {{
  const frameName = '{fn_safe}';
  const headline = '{hl_safe}';
  const subheadline = '{sub_safe}';
  const earnings = '{earn_safe}';
  const photoData = 'data:image/png;base64,{photo_base64}';

  // Create frame
  const frame = figma.createFrame();
  frame.name = frameName;
  frame.resize(1200, 1200);
  frame.fills = [{{ type: 'SOLID', color: {{ r: 1, g: 1, b: 1 }} }}];

  // Background photo as image
  const photoBlob = await fetch(photoData).then(r => r.blob());
  const photoBytes = await photoBlob.arrayBuffer();
  const photoImage = figma.createImage(new Uint8Array(photoBytes));
  const photoPaint = {{ type: 'IMAGE', imageHash: photoImage.hash, scaleMode: 'FILL' }};
  const photoBg = figma.createRectangle();
  photoBg.resize(1200, 1200);
  photoBg.fills = [photoPaint];
  photoBg.name = "Photo";
  frame.appendChild(photoBg);

  // Gradient overlay rects (vector shapes)
  const gradA = figma.createRectangle();
  gradA.resize(1120, 992);
  gradA.x = 40;
  gradA.y = 40;
  gradA.fills = [{{ type: 'SOLID', color: {{ r: 0.68, g: 0.85, b: 0.90 }}, opacity: 0.4 }}];
  gradA.cornerRadius = 30;
  gradA.name = "Gradient A";
  frame.appendChild(gradA);

  const gradB = figma.createRectangle();
  gradB.resize(1120, 992);
  gradB.x = 40;
  gradB.y = 40;
  gradB.fills = [{{ type: 'SOLID', color: {{ r: 1.0, g: 0.71, b: 0.76 }}, opacity: 0.45 }}];
  gradB.cornerRadius = 30;
  gradB.name = "Gradient B";
  frame.appendChild(gradB);

  // Headline text
  const hlText = figma.createText();
  await figma.loadFontAsync({{ family: "Inter", style: "Bold" }});
  hlText.characters = headline;
  hlText.fontName = {{ family: "Inter", style: "Bold" }};
  hlText.fontSize = 86;
  hlText.fills = [{{ type: 'SOLID', color: {{ r: 1, g: 1, b: 1 }} }}];
  hlText.x = 40;
  hlText.y = 100;
  hlText.resize(1120, 208);
  hlText.textAlignHorizontal = "CENTER";
  hlText.name = "Headline";
  frame.appendChild(hlText);

  // Subheadline text
  const subText = figma.createText();
  await figma.loadFontAsync({{ family: "Inter", style: "Regular" }});
  subText.characters = subheadline;
  subText.fontName = {{ family: "Inter", style: "Regular" }};
  subText.fontSize = 46;
  subText.fills = [{{ type: 'SOLID', color: {{ r: 1, g: 1, b: 1 }} }}];
  subText.x = 40;
  subText.y = 853;
  subText.resize(1120, 56);
  subText.textAlignHorizontal = "CENTER";
  subText.name = "Subheadline";
  frame.appendChild(subText);

  // Bottom strip (white rect)
  const strip = figma.createRectangle();
  strip.resize(1200, 168);
  strip.y = 1032;
  strip.fills = [{{ type: 'SOLID', color: {{ r: 1, g: 1, b: 1 }} }}];
  strip.name = "Bottom Strip";
  frame.appendChild(strip);

  // Earnings text
  const earnText = figma.createText();
  await figma.loadFontAsync({{ family: "Inter", style: "Bold" }});
  earnText.characters = earnings;
  earnText.fontName = {{ family: "Inter", style: "Bold" }};
  earnText.fontSize = 32;
  earnText.fills = [{{ type: 'SOLID', color: {{ r: 0.24, g: 0.10, b: 0.0 }} }}];
  earnText.x = 60;
  earnText.y = 1066;
  earnText.name = "Earnings";
  frame.appendChild(earnText);

  const url = `https://www.figma.com/design/j16txqhVXak2TON1w5sdAH/?node-id=${{frame.id.replace(":", "-")}}`;
  figma.closePlugin(JSON.stringify({{ nodeId: frame.id, url }}));
}})();"""

    return js


def build_figma_clone_js(
    frame_name: str,
    headline: str,
    subheadline: str,
    earnings: str = "Earn $25–$50 USD per hour.",
) -> str:
    """
    Build JavaScript to clone the base Figma frame (20:2) and update text nodes.

    Args:
        frame_name: Target frame name, e.g. "abc123_A_v1"
        headline: Headline text (Inter Bold, white)
        subheadline: Subheadline text (Inter Regular, white)
        earnings: Earnings text for bottom strip (default: generic rate)

    Returns:
        JavaScript string ready to be passed to use_figma MCP.

    The script:
      1. Clones base frame 20:2 (named "69cf1a039ed66cc82e0fa8f3_A_v1")
      2. Renames clone to <frame_name>
      3. Updates text nodes by DFS index:
         [3] = headline
         [4] = subheadline
         [6] = earnings claim
      4. Returns { nodeId, url } on success
    """
    # Escape single quotes in strings for safe JavaScript embedding
    fn_safe = frame_name.replace("'", "\\'")
    hl_safe = headline.replace("'", "\\'").replace("\n", "\\n")
    sub_safe = subheadline.replace("'", "\\'").replace("\n", "\\n")
    earn_safe = earnings.replace("'", "\\'")

    js = f"""(async () => {{
  const BASE_ID = "20:2";
  const frameName = '{fn_safe}';
  const headline = '{hl_safe}';
  const subheadline = '{sub_safe}';
  const earnings = '{earn_safe}';

  const base = await figma.getNodeByIdAsync(BASE_ID);
  if (!base || base.type !== "FRAME") {{
    figma.closePlugin("Base frame not found");
    return;
  }}

  function dfs(root) {{
    const nodes = [];
    (function walk(n) {{ nodes.push(n); if ("children" in n) n.children.forEach(walk); }})(root);
    return nodes;
  }}

  const clone = base.clone();
  clone.name = frameName;
  clone.x = base.x + base.width + 80;

  const nodes = dfs(clone);
  const hlNode = nodes[3];   // headline text
  const subNode = nodes[4];  // subheadline text
  const earnNode = nodes[6]; // earnings text

  if (hlNode?.type === "TEXT") {{
    await figma.loadFontAsync({{ family: "Inter", style: "Bold" }});
    hlNode.characters = headline;
  }}
  if (subNode?.type === "TEXT") {{
    await figma.loadFontAsync({{ family: "Inter", style: "Regular" }});
    subNode.characters = subheadline;
  }}
  if (earnNode?.type === "TEXT") {{
    await figma.loadFontAsync({{ family: "Inter", style: "Bold" }});
    earnNode.characters = earnings;
  }}

  const url = `https://www.figma.com/design/j16txqhVXak2TON1w5sdAH/?node-id=${{clone.id.replace(":", "-")}}`;
  figma.closePlugin(JSON.stringify({{ nodeId: clone.id, url }}));
}})();"""

    return js
