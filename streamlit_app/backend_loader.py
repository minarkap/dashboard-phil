import sys
import inspect
from pathlib import Path


def load_backend_modules():
    """Carga segura de módulos del backend y devuelve tupla (funcs..., error).

    No aborta todo si falla un import; devuelve None para esa fuente y continúa.
    Mantiene la interfaz esperada por tabs_ingest: 11 elementos -> 10 funcs + error.
    (kajabi_tx, kajabi_subs_csv, hotmart_csv, stripe, ads, ga, hotmart_api, kajabi_api,
     attribution_sync, kajabi_subs_api, error)
    """
    ROOT_DIR = Path(__file__).resolve().parents[1]
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))

    # Inicializa
    kajabi_tx = kajabi_subs = hotmart_imp = None
    stripe_s = ads_s = ga_s = hotmart_s = kajabi_s = attrib_s = kajabi_subs_api = None
    any_ok = False
    first_error: Exception | None = None

    def _try(callable_loader):
        nonlocal any_ok, first_error
        try:
            v = callable_loader()
            if v is not None:
                any_ok = True
            # Envuelve para ignorar kwargs desconocidos (p.ej., insert_only)
            def _wrapped(*args, **kwargs):
                try:
                    sig = inspect.signature(v)
                    allowed = set(sig.parameters.keys())
                    filtered = {k: val for k, val in kwargs.items() if k in allowed}
                    return v(*args, **filtered)
                except Exception:
                    # Si falla introspección, llama tal cual
                    return v(*args, **kwargs)
            return _wrapped
        except Exception as e:  # noqa: BLE001
            if first_error is None:
                first_error = e
            return None

    kajabi_tx = _try(lambda: __import__("backend.import_kajabi_csv", fromlist=["import_transactions_csv"]).import_transactions_csv)
    kajabi_subs = _try(lambda: __import__("backend.import_kajabi_subscriptions_csv", fromlist=["import_subscriptions_csv"]).import_subscriptions_csv)
    hotmart_imp = _try(lambda: __import__("backend.import_hotmart_csv", fromlist=["import_hotmart_csv"]).import_hotmart_csv)
    stripe_s = _try(lambda: __import__("backend.etl.stripe_sync", fromlist=["run_stripe_sync"]).run_stripe_sync)
    ads_s = _try(lambda: __import__("backend.etl.ads_sync", fromlist=["run_ads_sync"]).run_ads_sync)
    ga_s = _try(lambda: __import__("backend.etl.ga_sync", fromlist=["run_ga_sync"]).run_ga_sync)
    hotmart_s = _try(lambda: __import__("backend.etl.hotmart_sync", fromlist=["run_hotmart_sync"]).run_hotmart_sync)
    kajabi_s = _try(lambda: __import__("backend.etl.kajabi_sync", fromlist=["run_kajabi_sync"]).run_kajabi_sync)
    attrib_s = _try(lambda: __import__("backend.etl.attribution_sync", fromlist=["run_attribution_sync"]).run_attribution_sync)
    kajabi_subs_api = _try(lambda: __import__("backend.etl.kajabi_sync", fromlist=["run_kajabi_subs_sync"]).run_kajabi_subs_sync)

    err = None if any_ok else first_error
    # Devuelve 11 elementos: (10 funcs, error)
    return (kajabi_tx, kajabi_subs, hotmart_imp, stripe_s, ads_s, ga_s, hotmart_s, kajabi_s, attrib_s, kajabi_subs_api, err)




