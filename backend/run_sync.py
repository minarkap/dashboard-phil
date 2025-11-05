import os
from dotenv import load_dotenv

from backend.db.config import init_db


def _try_imports():
    stripe = None
    ads = None
    ga = None
    hotmart = None
    kajabi = None
    sheets_loader = None
    # Stripe
    try:
        from backend.etl.stripe_sync import run_stripe_sync as _stripe
        stripe = _stripe
    except Exception:
        stripe = None
    # Ads (Google + Meta vía ads_sync)
    try:
        from backend.etl.ads_sync import run_ads_sync as _ads
        ads = _ads
    except Exception:
        ads = None
    # GA4 (Analytics)
    try:
        from backend.etl.ga_sync import run_ga_sync as _ga
        ga = _ga
    except Exception:
        ga = None
    # Hotmart
    try:
        from backend.etl.hotmart_sync import run_hotmart_sync as _hot
        hotmart = _hot
    except Exception:
        hotmart = None
    # Kajabi
    try:
        from backend.etl.kajabi_sync import run_kajabi_sync as _kaj
        kajabi = _kaj
    except Exception:
        kajabi = None
    # Google Sheets (economics)
    try:
        from streamlit_app.data import load_economics_from_sheets as _sheets
        sheets_loader = _sheets
    except Exception:
        sheets_loader = None
    return stripe, ads, ga, hotmart, kajabi, sheets_loader


def main():
    load_dotenv()
    init_db()

    stripe, ads, ga, hotmart, kajabi, sheets_loader = _try_imports()
    # Intentar cargar también la sync de suscripciones de Kajabi (si existe)
    run_kajabi_subs_sync = None
    try:
        from backend.etl.kajabi_sync import run_kajabi_subs_sync as _ksubs
        run_kajabi_subs_sync = _ksubs
    except Exception:
        run_kajabi_subs_sync = None

    results: list[str] = []

    # Sheets (no persiste en BD; sólo valida acceso)
    if sheets_loader is not None:
        try:
            econ = sheets_loader()
            results.append(f"Sheets OK ({len(econ) if isinstance(econ, dict) else 0} meses)")
        except Exception as e:
            results.append(f"Sheets error: {e}")
    else:
        results.append("Sheets no disponible")

    # GA4
    if ga is not None:
        try:
            n = ga(days_back=30)
            results.append(f"Analytics OK ({n} filas)")
        except Exception as e:
            results.append(f"Analytics error: {e}")
    else:
        results.append("Analytics no disponible")

    # Google Ads + Meta Ads
    if ads is not None:
        try:
            n_ads = ads(days_back=30, include_meta=True, include_google=True)
            results.append(f"Ads OK ({n_ads} filas)")
        except Exception as e:
            results.append(f"Ads error: {e}")
    else:
        results.append("Ads no disponible")

    # Hotmart
    if hotmart is not None:
        try:
            res_hot = hotmart(days_back=365)
            results.append(f"Hotmart OK ({res_hot})")
        except Exception as e:
            results.append(f"Hotmart error: {e}")
    else:
        results.append("Hotmart no disponible")

    # Kajabi
    if kajabi is not None:
        try:
            res = kajabi(days_back=365)
            results.append(f"Kajabi OK ({res})")
        except Exception as e:
            results.append(f"Kajabi error: {e}")
    else:
        results.append("Kajabi no disponible")

    # Kajabi suscripciones (opcional)
    if run_kajabi_subs_sync is not None:
        try:
            res_subs = run_kajabi_subs_sync()
            results.append(f"Kajabi Subs OK ({res_subs})")
        except Exception as e:
            results.append(f"Kajabi Subs error: {e}")

    # Stripe
    if stripe is not None:
        try:
            stripe()
            results.append("Stripe OK")
        except Exception as e:
            results.append(f"Stripe error: {e}")
    else:
        results.append("Stripe no disponible")

    print(" | ".join(results))


if __name__ == "__main__":
    main()


