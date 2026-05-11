"""
One-shot OAuth helper to mint a fresh LinkedIn access + refresh token with
the scopes our pipeline needs — most importantly `w_member_social`, which
unblocks DSC post creation (the static-ad creative attach path).

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/mint_linkedin_token.py

Flow:
  1. Script starts a local HTTP server on http://localhost:8765/callback.
  2. Script opens the LinkedIn consent URL in your default browser.
  3. Sign in as the LinkedIn user that owns the OCP app / ad account.
  4. The consent screen MUST list every scope below — click Allow.
  5. LinkedIn redirects to localhost:8765/callback?code=... and the script
     catches it.
  6. Script POSTs the code to /oauth/v2/accessToken to exchange it for an
     access token + refresh token.
  7. Verifies the returned `scope` field includes `w_member_social`.
  8. Prints `doppler secrets set` commands — nothing is written automatically.

Prerequisite — one-time LinkedIn app config:
  Open https://www.linkedin.com/developers/apps/<app-id>/auth and add
  `http://localhost:8765/callback` to "Authorized redirect URLs for your app".
  Without this, LinkedIn returns "Bummer, something went wrong" on consent.
"""
from __future__ import annotations

import json
import os
import re
import secrets
import sys
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


REDIRECT_HOST = "localhost"
REDIRECT_PORT = 8765
REDIRECT_PATH = "/callback"
REDIRECT_URI  = f"http://{REDIRECT_HOST}:{REDIRECT_PORT}{REDIRECT_PATH}"

# Full scope set this pipeline needs. Order matches LinkedIn's app config UI.
# - r_ads / r_ads_reporting / rw_ads: read+write campaign hierarchy
# - w_member_social: DSC post creation (creative attach)
# - openid / profile: OpenID Connect userinfo, lets us auto-discover the
#   token owner's urn:li:person URN immediately after mint so we can
#   keep LINKEDIN_MEMBER_URN in sync. Both ride on the "Sign In with
#   LinkedIn using OpenID Connect" product Tuan added 2026-05-08.
SCOPES = [
    "r_ads",
    "r_ads_reporting",
    "rw_ads",
    "w_member_social",
    "openid",
    "profile",
]

AUTHORIZE_URL = "https://www.linkedin.com/oauth/v2/authorization"
TOKEN_URL     = "https://www.linkedin.com/oauth/v2/accessToken"


class _CodeCaptureHandler(BaseHTTPRequestHandler):
    """Captures the `code` query param from the LinkedIn redirect."""

    captured: dict = {}

    def do_GET(self):  # noqa: N802 — http.server convention
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != REDIRECT_PATH:
            self.send_response(404)
            self.end_headers()
            return

        params = urllib.parse.parse_qs(parsed.query)
        _CodeCaptureHandler.captured = {
            "code":  (params.get("code") or [""])[0],
            "state": (params.get("state") or [""])[0],
            "error": (params.get("error") or [""])[0],
            "error_description": (params.get("error_description") or [""])[0],
        }

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if _CodeCaptureHandler.captured["error"]:
            body = (
                "<h2>LinkedIn OAuth error</h2>"
                f"<p><b>{_CodeCaptureHandler.captured['error']}</b>: "
                f"{_CodeCaptureHandler.captured['error_description']}</p>"
                "<p>Return to the terminal for details.</p>"
            )
        else:
            body = (
                "<h2>Authentication successful.</h2>"
                "<p>You can close this tab and return to the terminal.</p>"
            )
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format, *args):  # noqa: A002 — silence the default access log
        return


def main() -> int:
    client_id     = os.environ.get("LINKEDIN_CLIENT_ID", "").strip()
    client_secret = os.environ.get("LINKEDIN_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        print("ERROR: LINKEDIN_CLIENT_ID / LINKEDIN_CLIENT_SECRET not in env.")
        print("       Run this script under `doppler run --project outlier-campaign-agent --config dev --`")
        return 2

    import requests  # late import — already in requirements.txt

    state = secrets.token_urlsafe(24)
    auth_params = {
        "response_type": "code",
        "client_id":     client_id,
        "redirect_uri":  REDIRECT_URI,
        "scope":         " ".join(SCOPES),
        "state":         state,
    }
    auth_url = f"{AUTHORIZE_URL}?{urllib.parse.urlencode(auth_params)}"

    print("=" * 72)
    print(" LinkedIn OAuth — minting access + refresh token")
    print("=" * 72)
    print()
    print("Requested scopes:")
    for s in SCOPES:
        print(f"  - {s}")
    print()
    print(f"Redirect URI: {REDIRECT_URI}")
    print("(Must be registered in your LinkedIn app's Authorized redirect URLs)")
    print()
    print("Opening this URL in your default browser:")
    print(f"  {auth_url}")
    print()

    server = HTTPServer((REDIRECT_HOST, REDIRECT_PORT), _CodeCaptureHandler)
    try:
        webbrowser.open(auth_url)
        # Wait for exactly one redirect, then shut down.
        server.handle_request()
    finally:
        server.server_close()

    captured = _CodeCaptureHandler.captured
    if captured.get("error"):
        print(f"ERROR: LinkedIn returned {captured['error']}: {captured['error_description']}")
        return 3
    if not captured.get("code"):
        print("ERROR: no `code` returned from LinkedIn redirect.")
        return 3
    if captured.get("state") != state:
        print("ERROR: OAuth state mismatch — possible CSRF. Aborting.")
        return 3

    print("Got authorization code, exchanging for tokens…")
    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "authorization_code",
        "code":          captured["code"],
        "redirect_uri":  REDIRECT_URI,
        "client_id":     client_id,
        "client_secret": client_secret,
    })
    if not resp.ok:
        print(f"ERROR: token exchange failed (status {resp.status_code}):")
        print(resp.text[:500])
        return 4

    data = resp.json()
    access_token  = data.get("access_token", "")
    refresh_token = data.get("refresh_token", "")
    granted_scope = data.get("scope", "")
    expires_in    = data.get("expires_in", 0)
    refresh_exp   = data.get("refresh_token_expires_in", 0)

    if not access_token or not refresh_token:
        print("ERROR: missing access_token or refresh_token in response.")
        print(json.dumps(data, indent=2)[:500])
        return 4

    print()
    print("=== Granted scopes ===")
    # LinkedIn returns scopes as either space-separated or comma-separated
    # depending on the consent path. Handle both — split on any whitespace
    # OR comma, drop empties.
    granted = [s for s in re.split(r"[\s,]+", granted_scope or "") if s]
    for s in SCOPES:
        mark = "✓" if s in granted else "✗"
        print(f"  {mark} {s}")
    extras = [s for s in granted if s not in SCOPES]
    for s in extras:
        print(f"  + {s}  (extra)")

    if "w_member_social" not in granted:
        print()
        print("ERROR: minted token still lacks `w_member_social`.")
        print("       Check the LinkedIn consent screen on the next attempt;")
        print("       w_member_social must be visible in the scope list shown")
        print("       to the user. If it's not, the scope is not yet enabled")
        print("       on the LinkedIn app — re-check Products tab on the app.")
        return 5

    print()
    print(f"Access token expires in:  {expires_in} sec  (~{expires_in // 86400} days)")
    print(f"Refresh token expires in: {refresh_exp} sec (~{refresh_exp // 86400} days)")

    # ── Discover the token owner's urn:li:person via /v2/userinfo ───────────
    # Requires `openid` + `profile` scopes (added above). The `sub` field is
    # the LinkedIn member's internal ID — same shape as the URN suffix we
    # need for LINKEDIN_MEMBER_URN.
    member_urn = ""
    try:
        ui_resp = requests.get(
            "https://api.linkedin.com/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        if ui_resp.ok:
            ui = ui_resp.json()
            sub = ui.get("sub", "").strip()
            name = ui.get("name", "")
            email = ui.get("email", "")
            if sub:
                member_urn = f"urn:li:person:{sub}"
                print()
                print(f"Token owner identity (from /v2/userinfo):")
                print(f"  name:  {name or '(not provided)'}")
                print(f"  email: {email or '(not provided)'}")
                print(f"  URN:   {member_urn}")
        else:
            print()
            print(f"WARN: /v2/userinfo returned {ui_resp.status_code}: {ui_resp.text[:300]}")
            print("      LINKEDIN_MEMBER_URN cannot be auto-discovered. You'll need to")
            print("      look it up manually from LinkedIn UI before DSC posts will work.")
    except Exception as exc:  # noqa: BLE001
        print()
        print(f"WARN: /v2/userinfo call failed ({exc}). LINKEDIN_MEMBER_URN auto-discovery skipped.")

    print()
    print("=" * 72)
    print(" SUCCESS — minted token has w_member_social.")
    print("=" * 72)
    print()
    print("Run these to save them to Doppler:")
    print()
    # Single-quote tokens to be shell-safe (LinkedIn tokens are URL-safe).
    print(f"  doppler secrets set LINKEDIN_ACCESS_TOKEN='{access_token}' \\")
    print(f"    --project outlier-campaign-agent --config dev")
    print()
    print(f"  doppler secrets set LINKEDIN_REFRESH_TOKEN='{refresh_token}' \\")
    print(f"    --project outlier-campaign-agent --config dev")
    if member_urn:
        print()
        print(f"  doppler secrets set LINKEDIN_MEMBER_URN='{member_urn}' \\")
        print(f"    --project outlier-campaign-agent --config dev")
    print()
    print("Then re-run the static-ad smoke test:")
    print()
    print("  DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib TEST_PLATFORMS=linkedin \\")
    print("    doppler run --project outlier-campaign-agent --config dev -- \\")
    print("    venv/bin/python scripts/test_cardiologist_3channel.py")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
