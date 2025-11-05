from google_auth_oauthlib.flow import InstalledAppFlow
import os

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']  # Para Ads usa 'https://www.googleapis.com/auth/adwords'

# Cargar credenciales desde variables de entorno para evitar exponer secretos en el código
client_id = (
  os.getenv("GA4_OAUTH_CLIENT_ID")
  or os.getenv("OAUTH_CLIENT_ID")
  or os.getenv("GOOGLE_ADS_CLIENT_ID")
)
client_secret = (
  os.getenv("GA4_OAUTH_CLIENT_SECRET")
  or os.getenv("OAUTH_CLIENT_SECRET")
  or os.getenv("GOOGLE_ADS_CLIENT_SECRET")
)

if not client_id or not client_secret:
  raise SystemExit("Faltan OAUTH_CLIENT_ID/SECRET (o variantes GA4_/GOOGLE_ADS_) en el entorno")

config = {
  "installed": {
    "client_id": client_id,
    "client_secret": client_secret,
    "redirect_uris": ["http://localhost"],
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}

flow = InstalledAppFlow.from_client_config(config, SCOPES)
creds = flow.run_local_server(port=0)
print("REFRESH_TOKEN=", creds.refresh_token)