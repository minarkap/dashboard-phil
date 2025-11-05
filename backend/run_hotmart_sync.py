import os
from dotenv import load_dotenv

from backend.db.config import init_db
from backend.etl.hotmart_sync import run_hotmart_sync


def main():
    load_dotenv()
    init_db()
    if not (os.getenv("HOTMART_ACCESS_TOKEN") or (os.getenv("HOTMART_CLIENT_ID") and os.getenv("HOTMART_CLIENT_SECRET"))):
        raise SystemExit("Configura HOTMART_ACCESS_TOKEN o HOTMART_CLIENT_ID/SECRET en .env")
    run_hotmart_sync()


if __name__ == "__main__":
    main()


