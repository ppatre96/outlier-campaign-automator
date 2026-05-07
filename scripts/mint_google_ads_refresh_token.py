"""
One-shot OAuth helper to mint a Google Ads refresh token with the right scope.

The existing GOOGLE_ADS_REFRESH_TOKEN in Doppler was minted for Tag Manager
+ Analytics scopes — Google Ads API calls fail with
ACCESS_TOKEN_SCOPE_INSUFFICIENT. This script runs an InstalledAppFlow to
mint a fresh token with `https://www.googleapis.com/auth/adwords` and
prints the command to update Doppler.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/mint_google_ads_refresh_token.py

Flow:
  1. Script prints a Google consent URL and opens it in your default browser.
  2. Sign in as the user who has access to Google Ads customer 6301406350.
  3. The consent screen MUST show "Manage your AdWords campaigns" — click Allow.
  4. Browser redirects to localhost:<port>/?code=... and the script catches it.
  5. Script exchanges the code for a refresh token + access token.
  6. Verifies the access token has the `adwords` scope.
  7. Prints the `doppler secrets set` command for you to run.

Nothing is written automatically — you copy/paste the final command yourself.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> int:
    client_id     = os.environ.get("GOOGLE_ADS_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        print("ERROR: GOOGLE_ADS_CLIENT_ID / GOOGLE_ADS_CLIENT_SECRET not in env.")
        print("       Run this script under `doppler run --project outlier-campaign-agent --config dev --`")
        return 2

    # Late import — google-auth-oauthlib is already in requirements.txt.
    from google_auth_oauthlib.flow import InstalledAppFlow
    import requests

    SCOPES = ["https://www.googleapis.com/auth/adwords"]

    flow = InstalledAppFlow.from_client_config(
        {
            "installed": {
                "client_id":     client_id,
                "client_secret": client_secret,
                "auth_uri":      "https://accounts.google.com/o/oauth2/auth",
                "token_uri":     "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost"],
            }
        },
        scopes=SCOPES,
    )

    print("=" * 72)
    print(" Google Ads OAuth — minting a refresh token with `adwords` scope")
    print("=" * 72)
    print()
    print("Your default browser will open. Sign in as the user who has")
    print("access to Google Ads customer 6301406350. On the consent screen")
    print("you should see 'Manage your AdWords campaigns' — click Allow.")
    print()

    # `prompt='consent'` forces a fresh consent screen even if user previously
    # authorized this app for other scopes — guarantees a refresh_token is
    # returned (Google omits refresh_token on subsequent silent re-auths).
    creds = flow.run_local_server(
        port=0,
        prompt="consent",
        access_type="offline",
        authorization_prompt_message=(
            "Open the following URL in a browser:\n  {url}\n"
        ),
        success_message=(
            "Authentication successful. You can close this tab and return to the terminal."
        ),
    )

    if not creds.refresh_token:
        print()
        print("ERROR: Google did not return a refresh_token.")
        print("       This typically happens when the user has previously")
        print("       consented and Google reuses the existing grant. Try")
        print("       revoking the app at https://myaccount.google.com/permissions")
        print("       and re-running.")
        return 3

    # Verify the new token has the right scope.
    print()
    print("=== Verifying scopes on the minted access token ===")
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id":     client_id,
        "client_secret": client_secret,
        "refresh_token": creds.refresh_token,
        "grant_type":    "refresh_token",
    })
    if resp.status_code != 200:
        print(f"WARNING: scope verification failed (status {resp.status_code}):")
        print(resp.text[:500])
    else:
        scope_str = resp.json().get("scope", "")
        scopes = scope_str.split()
        ok = "https://www.googleapis.com/auth/adwords" in scopes
        for s in scopes:
            mark = "✓" if "adwords" in s else " "
            print(f"  {mark} {s}")
        if not ok:
            print()
            print("ERROR: minted token still lacks the `adwords` scope.")
            print("       Check the consent screen carefully on the next attempt.")
            return 4

    print()
    print("=" * 72)
    print(" SUCCESS — refresh token has `adwords` scope.")
    print("=" * 72)
    print()
    print("Run this to save it to Doppler:")
    print()
    # Single-quote the token to be shell-safe (refresh tokens are URL-safe
    # base64 with `/`, `_`, `-` — no single quotes).
    print(f"  doppler secrets set GOOGLE_ADS_REFRESH_TOKEN='{creds.refresh_token}' \\")
    print(f"    --project outlier-campaign-agent --config dev")
    print()
    print("Then re-run the test:")
    print()
    print("  DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib TEST_PLATFORMS=google \\")
    print("    doppler run --project outlier-campaign-agent --config dev -- \\")
    print("    venv/bin/python scripts/test_cardiologist_3channel.py")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
