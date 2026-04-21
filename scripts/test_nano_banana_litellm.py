"""
Test nano banana (Gemini image generation) via Scale's LiteLLM proxy.

Confirmed working models (all via /images/generations endpoint):
  - gemini/gemini-2.5-flash-image     (nano banana, 1.8MB output)
  - gemini/imagen-4.0-generate-001    (Imagen 4, 1.4MB, highest quality)
  - gemini/imagen-4.0-fast-generate-001  (Imagen 4 Fast, 1.2MB)
  - gemini/gemini-3.1-flash-image-preview  (0.85MB)

Run:
    cd /Users/pranavpatre/outlier-campaign-agent
    source venv/bin/activate
    PYTHONPATH=. python scripts/test_nano_banana_litellm.py
"""
import base64
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import config  # noqa: E402
import requests  # noqa: E402

LITELLM_BASE = config.LITELLM_BASE_URL
API_KEY      = config.LITELLM_API_KEY

if not API_KEY:
    print("ERROR: LITELLM_API_KEY is not set in .env")
    sys.exit(1)

headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

PROMPT = (
    "A software developer at a coding desk, dual monitors with IDE showing "
    "colorful Python code, warm home office, bookshelf, plants, natural window "
    "light, editorial lifestyle photography, shallow depth of field."
)

# ── Test 1: text via Gemini (sanity check) ─────────────────────────────────────
print("=" * 60)
print("Test 1 — Gemini text via LiteLLM proxy")
r = requests.post(
    f"{LITELLM_BASE}/chat/completions",
    headers=headers,
    json={"model": "gemini/gemini-2.5-flash", "messages": [{"role": "user", "content": "Say hello in one word."}]},
    timeout=30,
)
print(f"  Status   : {r.status_code}")
if r.ok:
    print(f"  Response : {r.json()['choices'][0]['message']['content']}")
    print("  PASSED")
else:
    print(f"  Error    : {r.text[:300]}")
    sys.exit(1)

# ── Test 2: nano banana image via /images/generations ─────────────────────────
print()
print("=" * 60)
print("Test 2 — nano banana (gemini-2.5-flash-image) via /images/generations")
r2 = requests.post(
    f"{LITELLM_BASE}/images/generations",
    headers=headers,
    json={"model": "gemini/gemini-2.5-flash-image", "prompt": PROMPT, "n": 1},
    timeout=120,
)
print(f"  Status   : {r2.status_code}")
if r2.ok:
    imgs = r2.json().get("data", [])
    if imgs and "b64_json" in imgs[0]:
        out = Path("/tmp/nano_banana_test.png")
        out.write_bytes(base64.b64decode(imgs[0]["b64_json"]))
        print(f"  Saved    : {out} ({out.stat().st_size:,} bytes)")
        print("  PASSED")
    else:
        print(f"  No b64_json in response: {str(r2.json())[:300]}")
        sys.exit(1)
else:
    print(f"  Error    : {r2.text[:400]}")
    sys.exit(1)

# ── Test 3: Imagen 4 (higher quality option) ────────────────────────────────────
print()
print("=" * 60)
print("Test 3 — Imagen 4 (imagen-4.0-generate-001)")
r3 = requests.post(
    f"{LITELLM_BASE}/images/generations",
    headers=headers,
    json={"model": "gemini/imagen-4.0-generate-001", "prompt": PROMPT, "n": 1},
    timeout=120,
)
print(f"  Status   : {r3.status_code}")
if r3.ok:
    imgs = r3.json().get("data", [])
    if imgs and "b64_json" in imgs[0]:
        out = Path("/tmp/imagen4_test.png")
        out.write_bytes(base64.b64decode(imgs[0]["b64_json"]))
        print(f"  Saved    : {out} ({out.stat().st_size:,} bytes)")
        print("  PASSED")
    else:
        print(f"  Response: {str(r3.json())[:300]}")
else:
    print(f"  Error    : {r3.text[:300]}")

print()
print("All tests done. Open /tmp/nano_banana_test.png and /tmp/imagen4_test.png to compare.")
