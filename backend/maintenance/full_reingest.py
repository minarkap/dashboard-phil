from __future__ import annotations

import os
from typing import Dict

from dotenv import load_dotenv

from backend.db.config import init_db


def _try(fn, *args, **kwargs) -> str:
    try:
        res = fn(*args, **kwargs)
        return f"OK: {res}" if res is not None else "OK"
    except Exception as e:  # noqa: BLE001
        return f"error: {e}"


def main() -> None:
    load_dotenv()
    init_db()

    # Import tardío para evitar fallos si faltan deps; el loader ya filtra kwargs desconocidos
    try:
        from streamlit_app.backend_loader import load_backend_modules
    except Exception:
        load_backend_modules = None  # type: ignore

    results: Dict[str, str] = {}

    if load_backend_modules is not None:
        mods = load_backend_modules()
        # Soporta retornos de 9 (nuevo) o 10 (antiguo con attribution)
        if not isinstance(mods, (list, tuple)) or len(mods) < 9:
            raise RuntimeError("load_backend_modules devolvió un resultado inesperado")
        kajabi_import_tx = mods[0]
        kajabi_import_subs = mods[1]
        hotmart_import = mods[2]
        run_stripe_sync = mods[3]
        run_ads_sync = mods[4]
        run_ga_sync = mods[5]
        run_hotmart_sync = mods[6]
        run_kajabi_sync = mods[7]
        _err = mods[8]
        run_attribution_sync = None
        run_kajabi_subs_sync = None
        if len(mods) >= 10:
            run_attribution_sync = mods[9]
        if len(mods) >= 11:
            run_kajabi_subs_sync = mods[10]

        # Orden lógico: pagos (Stripe/Kajabi/Hotmart) + analítica (GA4/Ads) + Sheets
        if run_kajabi_sync is not None:
            results["kajabi_api"] = _try(run_kajabi_sync, days_back=365)
        if run_hotmart_sync is not None:
            results["hotmart_api"] = _try(run_hotmart_sync, days_back=365)
        if run_stripe_sync is not None:
            results["stripe"] = _try(run_stripe_sync)
        if run_ga_sync is not None:
            results["ga4"] = _try(run_ga_sync, days_back=365)
        if run_ads_sync is not None:
            results["ads"] = _try(run_ads_sync, days_back=365, include_meta=True, include_google=True)
        if run_kajabi_subs_sync is not None:
            results["kajabi_subs_api"] = _try(run_kajabi_subs_sync)

    # Sheets (economics) no persiste, sólo chequeo opcional
    try:
        from streamlit_app.data import load_economics_from_sheets
        econ = load_economics_from_sheets()
        results["sheets"] = f"OK ({len(econ)} meses)" if isinstance(econ, dict) else "OK"
    except Exception as e:  # noqa: BLE001
        results["sheets"] = f"error: {e}"

    print(" | ".join(f"{k} {v}" for k, v in results.items()))


if __name__ == "__main__":
    main()


