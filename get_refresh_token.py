from __future__ import annotations

import os
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv


def main():
    # Carga .env si existe
    load_dotenv()

    client_id = os.getenv("OAUTH_CLIENT_ID") or os.getenv("GA4_OAUTH_CLIENT_ID") or os.getenv("GOOGLE_ADS_CLIENT_ID")
    client_secret = os.getenv("OAUTH_CLIENT_SECRET") or os.getenv("GA4_OAUTH_CLIENT_SECRET") or os.getenv("GOOGLE_ADS_CLIENT_SECRET")
    scope = os.getenv("OAUTH_SCOPE", "https://www.googleapis.com/auth/analytics.readonly")

    if not client_id or not client_secret:
        raise SystemExit("Faltan OAUTH_CLIENT_ID y OAUTH_CLIENT_SECRET (o GA4_OAUTH_CLIENT_ID/SECRET)")

    cfg = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uris": ["http://localhost"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }

    flow = InstalledAppFlow.from_client_config(cfg, scopes=[scope])
    creds = flow.run_local_server(port=0)
    print("REFRESH_TOKEN=", creds.refresh_token)


if __name__ == "__main__":
    main()


