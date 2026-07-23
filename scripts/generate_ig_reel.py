"""
Generate a 24-second vertical IG Reel (9:16, 1080x1920) for GMR-PRECISION-MX-2026-05-26.

3 clips x 8 seconds (A / B / C angles) stitched via ffmpeg, plus a 2-second end card.
Video generated via Veo 3 (veo-3.0-generate-001) using the Gemini predictLongRunning API.
"""
import io
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import requests

# ── Setup ─────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("reel_gen")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402 — loads after sys.path fix

RAMP_ID = "GMR-PRECISION-MX-2026-05-26"
CDMX_FOLDER_ID = "1nJWT9LYuVbHDMvlS5hIJ9_2PEIFVWmy7"
VEO_MODEL = "veo-3.0-generate-001"
VEO_FALLBACK = "veo-2.0-generate-001"
POLL_INTERVAL_S = 12
MAX_POLL_ATTEMPTS = 60  # 12 min max per clip
MAX_RETRIES = 1  # 1 retry per failed clip

# ffmpeg / ffprobe — available via Remotion node_modules on this machine.
# Must be called with DYLD_LIBRARY_PATH set to the compositor dir so macOS
# can resolve the companion *.dylib files (libavcodec, libavdevice, etc.).
_REMOTION_BIN = Path("/Users/pranavpatre/remotion/node_modules/@remotion/compositor-darwin-arm64")
FFMPEG = str(_REMOTION_BIN / "ffmpeg")
FFPROBE = str(_REMOTION_BIN / "ffprobe")
_FF_ENV = {**os.environ, "DYLD_LIBRARY_PATH": str(_REMOTION_BIN)}

# Output paths
WORK_DIR = Path(tempfile.mkdtemp(prefix="reel_gen_"))
log.info("Work dir: %s", WORK_DIR)

# ── Motion prompts per angle ──────────────────────────────────────────────────
# Drawn from brief.md visual direction + ICP psychographic
MOTION_BRIEFS = {
    "A": (
        "Vertical 9:16 video. A Mexican man in his early 30s sits at a tidy home-office desk "
        "in a CDMX urban apartment, reviewing a structured handwritten checklist in a small "
        "notebook with quiet focus. Natural morning light from a window on the left. The camera "
        "opens with a medium shot showing the organized desk and notebook, then performs a slow, "
        "deliberate push-in toward his face and hands as he reads each line carefully. "
        "Mood: composed, methodical, calm confidence — zero rush. Shallow depth of field. "
        "Warm ambient tones, neutral background wall, no clutter. "
        "No text overlays. No logos. No fast cuts. Duration 8 seconds."
    ),
    "B": (
        "Vertical 9:16 video. A Mexican Indigenous-heritage woman in her late 20s sits at a "
        "neatly organized co-working desk in CDMX with color-coded folders and labeled file tabs "
        "visible in the background. She holds a pen and methodically reviews a structured "
        "document, turning pages deliberately. The camera tracks smoothly left-to-right at "
        "desk level, revealing the organized workspace — files, stationery in order, clean lines. "
        "Mood: structured, precise, professional calm. Soft natural light from large windows "
        "behind her. Smooth lateral tracking motion, no jump cuts. "
        "No text overlays. No logos. Duration 8 seconds."
    ),
    "C": (
        "Vertical 9:16 video. Close-up of a Mexican man in his early 30s at a minimal home-office "
        "desk. He is reading text on a laptop screen and gently places his index finger on a "
        "specific line, catching a small detail. His expression shifts from focused concentration "
        "to quiet satisfaction — a subtle moment of 'I noticed that'. The camera starts slightly "
        "wide to show the organized desk, then a gentle rack-focus pulls from the laptop screen "
        "to his face in the final 3 seconds, emphasizing the moment of recognition. "
        "Warm side light from a window. Calm, self-assured mood. No high energy. "
        "No text overlays. No logos. Duration 8 seconds."
    ),
}

# End card text (using Angle C headline for emotional payoff per brief)
END_CARD_HEADLINE = "Notas lo que otros ignoran"
END_CARD_CTA = "Regístrate en Outlier"
END_CARD_SOURCE_ANGLE = "C"


# ── Veo 3 API helpers ─────────────────────────────────────────────────────────

def _veo_submit(prompt: str, model: str, api_key: str) -> str:
    """Submit a Veo generation job. Returns the operation name."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:predictLongRunning?key={api_key}"
    payload = {
        "instances": [{"prompt": prompt}],
        "parameters": {
            "aspectRatio": "9:16",
            "durationSeconds": 8,
            "sampleCount": 1,
        },
    }
    resp = requests.post(url, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Veo submit error {resp.status_code}: {resp.text[:400]}")
    data = resp.json()
    op_name = data.get("name")
    if not op_name:
        raise RuntimeError(f"Veo submit: no operation name in response: {data}")
    log.info("Veo operation submitted: %s", op_name)
    return op_name


def _veo_poll(op_name: str, api_key: str) -> dict:
    """
    Poll a Veo operation until done. Returns the final response dict.
    Raises RuntimeError on timeout or API error.
    """
    url = f"https://generativelanguage.googleapis.com/v1beta/{op_name}?key={api_key}"
    for attempt in range(MAX_POLL_ATTEMPTS):
        time.sleep(POLL_INTERVAL_S)
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Veo poll error {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        done = data.get("done", False)
        log.info("Poll %d/%d — done=%s", attempt + 1, MAX_POLL_ATTEMPTS, done)
        if done:
            if "error" in data:
                raise RuntimeError(f"Veo operation failed: {data['error']}")
            return data
    raise RuntimeError(f"Veo operation timed out after {MAX_POLL_ATTEMPTS * POLL_INTERVAL_S}s")


def _veo_download(result: dict, out_path: Path, api_key: str) -> Path:
    """
    Extract video bytes from a completed Veo operation result and save to out_path.

    Veo 3 actual response shape (observed 2026-05-26):
      response.generateVideoResponse.generatedSamples[].video.uri
      → Files API download URL: https://generativelanguage.googleapis.com/v1beta/files/<id>:download?alt=media

    Legacy shapes also handled:
      response.predictions[].bytesBase64Encoded
      response.predictions[].gcsUri
    """
    resp_body = result.get("response", {})

    # ── Primary: Veo 3 Files API URI ──────────────────────────────────────────
    samples = resp_body.get("generateVideoResponse", {}).get("generatedSamples", [])
    if samples:
        video_uri = samples[0].get("video", {}).get("uri", "")
        if video_uri:
            log.info("Veo Files API URI: %s — downloading...", video_uri)
            # Append API key if not already in URL
            dl_url = video_uri if "key=" in video_uri else f"{video_uri}&key={api_key}"
            r = requests.get(dl_url, timeout=180, stream=True)
            if r.status_code == 200:
                with open(out_path, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        fh.write(chunk)
                log.info("Saved video (Files API) to %s (%d bytes)", out_path, out_path.stat().st_size)
                return out_path
            raise RuntimeError(f"Could not download Veo video from Files API: HTTP {r.status_code} — {r.text[:200]}")

    # ── Fallback: legacy predictions[] shape ──────────────────────────────────
    predictions = resp_body.get("predictions", [])
    if not predictions:
        raise RuntimeError(f"Veo result has no predictions or generatedSamples: {json.dumps(result)[:600]}")

    pred = predictions[0]

    # Case 1: base64-encoded bytes
    if "bytesBase64Encoded" in pred:
        import base64
        video_bytes = base64.b64decode(pred["bytesBase64Encoded"])
        out_path.write_bytes(video_bytes)
        log.info("Saved video (base64) to %s (%d bytes)", out_path, len(video_bytes))
        return out_path

    # Case 2: GCS URI
    if "gcsUri" in pred:
        gcs_uri = pred["gcsUri"]
        log.info("GCS URI: %s — downloading...", gcs_uri)
        gcs_http = gcs_uri.replace("gs://", "https://storage.googleapis.com/")
        r = requests.get(gcs_http, timeout=180)
        if r.status_code == 200:
            out_path.write_bytes(r.content)
            log.info("Saved video (GCS) to %s (%d bytes)", out_path, len(r.content))
            return out_path
        raise RuntimeError(f"Could not download GCS video {gcs_uri}: HTTP {r.status_code}")

    raise RuntimeError(f"Cannot extract video from prediction: {json.dumps(pred)[:400]}")


def generate_clip(angle: str, api_key: str, model: str = VEO_MODEL) -> tuple[Path, str, int]:
    """
    Generate a single 8-second clip for the given angle.
    Returns (clip_path, model_used, retry_count).
    """
    prompt = MOTION_BRIEFS[angle]
    retry_count = 0
    last_err = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            log.info("[Angle %s] Submitting clip (attempt %d, model=%s)...", angle, attempt + 1, model)
            t0 = time.time()
            op_name = _veo_submit(prompt, model, api_key)
            result = _veo_poll(op_name, api_key)
            elapsed = int(time.time() - t0)
            log.info("[Angle %s] Done in %ds", angle, elapsed)
            out_path = WORK_DIR / f"clip_{angle}.mp4"
            _veo_download(result, out_path, api_key)
            return out_path, model, retry_count
        except Exception as exc:
            last_err = exc
            log.warning("[Angle %s] Attempt %d failed: %s", angle, attempt + 1, exc)
            retry_count += 1
            if attempt == 0 and model == VEO_MODEL:
                log.info("[Angle %s] Retrying with same model after failure", angle)
            continue

    raise RuntimeError(f"Angle {angle} failed after {MAX_RETRIES + 1} attempts: {last_err}")


# ── End card generation ────────────────────────────────────────────────────────

def build_end_card(source_clip: Path, headline: str, cta: str) -> Path:
    """
    Build a 2-second end card by:
    1. Extracting the last frame from source_clip (Angle C)
    2. Using ffmpeg drawtext to overlay headline + CTA + Outlier wordmark
    3. Looping that still for 2 seconds with a fade-in
    Returns path to 2-second end-card mp4.
    """
    out_path = WORK_DIR / "end_card.mp4"

    # Extract last frame as PNG
    still_path = WORK_DIR / "end_card_still.png"
    subprocess.run(
        [
            FFMPEG, "-y",
            "-sseof", "-0.1",
            "-i", str(source_clip),
            "-vframes", "1",
            "-q:v", "2",
            str(still_path),
        ],
        check=True,
        capture_output=True,
        env=_FF_ENV,
    )
    log.info("End card still extracted: %s", still_path)

    # Find a font — use system fonts available on macOS
    font_candidates = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
        "/System/Library/Fonts/SFNSDisplay.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
    ]
    font_path = None
    for fp in font_candidates:
        if Path(fp).exists():
            font_path = fp
            break
    if font_path is None:
        font_path = "/System/Library/Fonts/Helvetica.ttc"

    # Dark gradient overlay + text via ffmpeg
    # drawtext: headline centered at ~40% height, CTA at ~55%, Outlier at ~70%
    headline_escaped = headline.replace("'", "\\'").replace(":", "\\:")
    cta_escaped = cta.replace("'", "\\'").replace(":", "\\:")

    vf = (
        # Dark overlay for readability
        f"[in]colorchannelmixer=aa=0.7[dark];"
        f"color=c=black@0.55:s=1080x1920[overlay];"
        f"[overlay][dark]overlay=0:0[base];"
        # Headline
        f"[base]drawtext=fontfile='{font_path}'"
        f":text='{headline_escaped}'"
        f":fontsize=72:fontcolor=white:x=(w-text_w)/2:y=h*0.40"
        f":line_spacing=10[hl];"
        # CTA
        f"[hl]drawtext=fontfile='{font_path}'"
        f":text='{cta_escaped}'"
        f":fontsize=52:fontcolor=white:x=(w-text_w)/2:y=h*0.55[cta];"
        # Outlier wordmark
        f"[cta]drawtext=fontfile='{font_path}'"
        f":text='Outlier'"
        f":fontsize=48:fontcolor=white@0.8:x=(w-text_w)/2:y=h*0.68[out]"
    )

    # Simpler approach — avoid complex filter chain issues with ffmpeg escaping
    # Use a two-pass: first darken still, then add text
    darkened_path = WORK_DIR / "end_card_dark.png"
    subprocess.run(
        [
            FFMPEG, "-y",
            "-i", str(still_path),
            "-vf", "colorchannelmixer=rr=0.4:gg=0.4:bb=0.4",
            str(darkened_path),
        ],
        check=True,
        capture_output=True,
        env=_FF_ENV,
    )

    # Build end card as a 2-second looped still with fade-in + drawtext
    drawtext_filters = ":".join([
        f"drawtext=fontfile='{font_path}'",
        f"text='{headline_escaped}'",
        "fontsize=72",
        "fontcolor=white",
        "x=(w-text_w)/2",
        "y=h*0.38",
        "alpha='if(lt(t,0.5),t/0.5,1)'",
    ])
    drawtext_cta = ":".join([
        f"drawtext=fontfile='{font_path}'",
        f"text='{cta_escaped}'",
        "fontsize=52",
        "fontcolor=white",
        "x=(w-text_w)/2",
        "y=h*0.54",
        "alpha='if(lt(t,0.5),t/0.5,1)'",
    ])
    drawtext_brand = ":".join([
        f"drawtext=fontfile='{font_path}'",
        "text='Outlier'",
        "fontsize=44",
        "fontcolor=white@0.85",
        "x=(w-text_w)/2",
        "y=h*0.66",
        "alpha='if(lt(t,0.5),t/0.5,1)'",
    ])

    subprocess.run(
        [
            FFMPEG, "-y",
            "-loop", "1",
            "-i", str(darkened_path),
            "-t", "2",
            "-vf", f"scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,{drawtext_filters},{drawtext_cta},{drawtext_brand}",
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            "-r", "24",
            str(out_path),
        ],
        check=True,
        capture_output=True,
        env=_FF_ENV,
    )
    log.info("End card built: %s", out_path)
    return out_path


# ── ffmpeg stitch ─────────────────────────────────────────────────────────────

def probe_video(clip_path: Path) -> dict:
    """Return basic video stream info from ffprobe."""
    result = subprocess.run(
        [
            FFPROBE, "-v", "error",
            "-print_format", "json",
            "-show_streams",
            str(clip_path),
        ],
        capture_output=True,
        text=True,
        env=_FF_ENV,
    )
    data = json.loads(result.stdout)
    streams = data.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), {})
    return {
        "codec": video.get("codec_name"),
        "width": video.get("width"),
        "height": video.get("height"),
        "r_frame_rate": video.get("r_frame_rate"),
        "duration": video.get("duration"),
    }


def normalize_clip(clip_path: Path, index: int) -> Path:
    """
    Re-encode clip to consistent 1080x1920 H.264 24fps yuv420p.
    Required before concat demuxer.
    """
    out_path = WORK_DIR / f"norm_{index}.mp4"
    subprocess.run(
        [
            FFMPEG, "-y",
            "-i", str(clip_path),
            "-vf", "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "18",
            "-pix_fmt", "yuv420p",
            "-r", "24",
            "-an",  # no audio
            str(out_path),
        ],
        check=True,
        capture_output=True,
        env=_FF_ENV,
    )
    log.info("Normalized clip %d: %s", index, out_path)
    return out_path


def stitch_clips(clip_paths: list[Path], out_path: Path) -> str:
    """
    Concatenate clips using ffmpeg concat demuxer.
    Returns the ffmpeg one-liner command used.
    """
    concat_list = WORK_DIR / "concat.txt"
    lines = "\n".join(f"file '{p.resolve()}'" for p in clip_paths)
    concat_list.write_text(lines)

    cmd = [
        FFMPEG, "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_list),
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-r", "24",
        "-movflags", "+faststart",
        str(out_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True, env=_FF_ENV)
    # Return the one-liner
    return " ".join(str(x) for x in cmd)


# ── Drive upload ───────────────────────────────────────────────────────────────

def upload_to_drive(local_path: Path, filename: str, folder_id: str, svc) -> str:
    """Upload a file to a specific Drive folder. Returns the web view URL."""
    from googleapiclient.http import MediaFileUpload

    file_metadata = {
        "name": filename,
        "parents": [folder_id],
    }
    media = MediaFileUpload(str(local_path), mimetype="video/mp4", resumable=True)
    file = svc.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,webViewLink",
        supportsAllDrives=True,
    ).execute()
    drive_url = file.get("webViewLink", "")
    log.info("Uploaded %s to Drive: %s", filename, drive_url)
    return drive_url


def upload_clip_to_drive(local_path: Path, filename: str, folder_id: str, svc) -> str:
    """Upload individual clip mp4. Returns web view URL."""
    return upload_to_drive(local_path, filename, folder_id, svc)


def update_manifest_in_drive(manifest: dict, folder_id: str, svc) -> str:
    """
    Upload updated manifest.json to Drive, replacing or creating the file.
    Returns web view URL.
    """
    from googleapiclient.http import MediaIoBaseUpload

    # Find existing manifest.json or manifest.png
    results = svc.files().list(
        q=f"'{folder_id}' in parents and name='manifest.json' and trashed=false",
        spaces="drive",
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    existing = results.get("files", [])

    json_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    media = MediaIoBaseUpload(io.BytesIO(json_bytes), mimetype="application/json", resumable=False)

    if existing:
        file = svc.files().update(
            fileId=existing[0]["id"],
            media_body=media,
            fields="id,webViewLink",
            supportsAllDrives=True,
        ).execute()
    else:
        file = svc.files().create(
            body={"name": "manifest.json", "parents": [folder_id]},
            media_body=media,
            fields="id,webViewLink",
            supportsAllDrives=True,
        ).execute()

    url = file.get("webViewLink", "")
    log.info("Manifest uploaded: %s", url)
    return url


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    import config
    api_key = config.GEMINI_API_KEY
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    from src.gdrive import _service
    svc = _service()

    # Existing manifest (use second copy — it has the most complete C variant)
    existing_manifest = {
        "ramp_id": RAMP_ID,
        "channel": "meta",
        "cohort": "Precision Operator MX",
        "geo": "CDMX, Mexico City metro",
        "variants": {
            "A": {
                "drive_url": "https://drive.google.com/file/d/1DxoKdB_5Ez4cY66dzWaATD3XZAjzR0TF/view?usp=drivesdk",
                "copy": {
                    "headline": "Hecho bien desde la primera vez",
                    "subheadline": "Tareas que valoran tu nivel de detalle",
                    "ad_description": "+$500M USD pagados a colaboradores. Pautas claras, pago confiable.",
                },
            },
            "B": {
                "drive_url": "https://drive.google.com/file/d/14Uaay1gtiRBOlx492VhpPkW_-NV2WfdC/view?usp=drivesdk",
                "copy": {
                    "headline": "Siempre sabrás qué se espera",
                    "subheadline": "Sin adivinar. Sin ambigüedad. Solo claridad.",
                    "ad_description": "Plataforma con +$500M USD en pagos. Únete a quienes trabajan con estándares.",
                },
            },
            "C": {
                "drive_url": "https://drive.google.com/file/d/1Pr9uj7BInBcD81MzjzMFpYntOCF0V70s/view?usp=drivesdk",
                "copy": {
                    "headline": "Notas lo que otros pasan por alto",
                    "subheadline": "Aquí eso marca la diferencia.",
                    "ad_description": "Pautas claras, pago confiable. +$500M USD pagados a colaboradores globales.",
                },
            },
        },
    }

    # ── Generate clips ─────────────────────────────────────────────────────────
    clip_results = {}
    generation_start = time.time()

    for angle in ["A", "B", "C"]:
        t_clip = time.time()
        log.info("=" * 60)
        log.info("Generating clip %s ...", angle)
        clip_path, model_used, retries = generate_clip(angle, api_key)
        gen_seconds = int(time.time() - t_clip)
        clip_results[angle] = {
            "path": clip_path,
            "model": model_used,
            "retries": retries,
            "gen_seconds": gen_seconds,
            "motion_brief_summary": MOTION_BRIEFS[angle][:120].replace("\n", " ") + "...",
        }
        log.info("Clip %s done in %ds (retries=%d)", angle, gen_seconds, retries)

    # ── Probe + normalize ──────────────────────────────────────────────────────
    log.info("Probing clips...")
    normalized = []
    for i, angle in enumerate(["A", "B", "C"]):
        info = probe_video(clip_results[angle]["path"])
        log.info("Clip %s: %s", angle, info)
        norm = normalize_clip(clip_results[angle]["path"], i)
        normalized.append(norm)

    # ── End card ───────────────────────────────────────────────────────────────
    log.info("Building end card from Angle C clip...")
    end_card_path = build_end_card(
        clip_results["C"]["path"],
        headline=END_CARD_HEADLINE,
        cta=END_CARD_CTA,
    )
    norm_end = normalize_clip(end_card_path, 99)
    normalized.append(norm_end)

    # ── Stitch ─────────────────────────────────────────────────────────────────
    reel_path = WORK_DIR / "reel.mp4"
    log.info("Stitching %d segments...", len(normalized))
    ffmpeg_cmd = stitch_clips(normalized, reel_path)

    # Verify final duration
    final_info = probe_video(reel_path)
    final_duration = float(final_info.get("duration", 0))
    log.info("Final reel: %s, duration=%.1fs", final_info, final_duration)

    total_runtime = int(time.time() - generation_start)

    # ── Upload to Drive ────────────────────────────────────────────────────────
    log.info("Uploading reel.mp4 to Drive...")
    reel_drive_url = upload_to_drive(reel_path, "reel.mp4", CDMX_FOLDER_ID, svc)

    # Upload individual normalized clips
    for angle in ["A", "B", "C"]:
        idx = {"A": 0, "B": 1, "C": 2}[angle]
        clip_drive_url = upload_clip_to_drive(
            normalized[idx],
            f"reel_clip_{angle}.mp4",
            CDMX_FOLDER_ID,
            svc,
        )
        clip_results[angle]["drive_url"] = clip_drive_url
        log.info("Clip %s uploaded: %s", angle, clip_drive_url)

    # ── Build updated manifest ─────────────────────────────────────────────────
    # Estimate cost: Veo 3 = ~$0.50/sec * 3 clips * 8s + negligible end card
    veo_cost_usd = round(3 * 8 * 0.50, 2)

    updated_manifest = existing_manifest.copy()
    updated_manifest["video"] = {
        "reel_drive_url": reel_drive_url,
        "duration_s": int(final_duration),
        "aspect": "9:16",
        "model": VEO_MODEL,
        "clips": [
            {
                "angle": angle,
                "motion_brief": MOTION_BRIEFS[angle],
                "veo_clip_drive_url": clip_results[angle].get("drive_url", ""),
                "gen_seconds": clip_results[angle]["gen_seconds"],
                "retry_count": clip_results[angle]["retries"],
                "veo_model": clip_results[angle]["model"],
            }
            for angle in ["A", "B", "C"]
        ],
        "end_card": {
            "headline": END_CARD_HEADLINE,
            "cta": END_CARD_CTA,
            "source_frame_from_angle": END_CARD_SOURCE_ANGLE,
        },
        "generation_cost_usd": veo_cost_usd,
        "ffmpeg_command": ffmpeg_cmd,
        "total_runtime_seconds": total_runtime,
    }

    manifest_drive_url = update_manifest_in_drive(updated_manifest, CDMX_FOLDER_ID, svc)

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("REEL GENERATION COMPLETE")
    print("=" * 70)
    summary = {
        "ramp_id": RAMP_ID,
        "asset": "ig_reel",
        "status": "SUCCESS",
        "reel_drive_url": reel_drive_url,
        "manifest_drive_url": manifest_drive_url,
        "duration_s": int(final_duration),
        "clips": {
            angle: {
                "veo_model": clip_results[angle]["model"],
                "motion_brief_summary": clip_results[angle]["motion_brief_summary"],
                "clip_drive_url": clip_results[angle].get("drive_url", ""),
                "gen_seconds": clip_results[angle]["gen_seconds"],
                "retry_count": clip_results[angle]["retries"],
            }
            for angle in ["A", "B", "C"]
        },
        "end_card": {
            "headline": END_CARD_HEADLINE,
            "cta": END_CARD_CTA,
            "source_frame_from_angle": END_CARD_SOURCE_ANGLE,
        },
        "veo_cost_usd": veo_cost_usd,
        "ffmpeg_command": ffmpeg_cmd,
        "total_runtime_seconds": total_runtime,
        "notes": f"Veo model: {VEO_MODEL}. End card: last frame of Clip C with fade-in text overlay.",
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    main()
