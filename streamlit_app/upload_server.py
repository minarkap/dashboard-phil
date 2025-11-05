from __future__ import annotations

import os
import tempfile
import threading
from pathlib import Path

from flask import Flask, request, jsonify

from .backend_loader import load_backend_modules


_server_started = False
_server_lock = threading.Lock()
_server_port = int(os.getenv("UPLOAD_SERVER_PORT", "8787"))


def _create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/health")
    def health():
        return jsonify({"ok": True})

    def _import_bytes(importer_callable, content: bytes) -> int:
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            return int(importer_callable(Path(tmp_path)) or 0)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def _handle_upload(importer_callable):
        if not importer_callable:
            return ("Importador no disponible", 500)
        if "file" not in request.files:
            return ("Falta campo 'file'", 400)
        f = request.files["file"]
        content = f.read()
        try:
            n = _import_bytes(importer_callable, content)
            return (f"OK - importadas {n} filas", 200)
        except Exception as e:
            return (f"Error: {e}", 500)

    @app.post("/upload/kajabi_tx")
    def upload_kajabi_tx():
        kajabi_tx, _, _, _, _ = load_backend_modules()
        return _handle_upload(kajabi_tx)

    @app.post("/upload/kajabi_subs")
    def upload_kajabi_subs():
        _, kajabi_subs, _, _, _ = load_backend_modules()
        return _handle_upload(kajabi_subs)

    @app.post("/upload/hotmart")
    def upload_hotmart():
        _, _, hotmart_imp, _, _ = load_backend_modules()
        return _handle_upload(hotmart_imp)

    return app


def ensure_server_running() -> str:
    global _server_started
    with _server_lock:
        if _server_started:
            return f"http://127.0.0.1:{_server_port}"

        app = _create_app()

        t = threading.Thread(
            target=lambda: app.run(host="127.0.0.1", port=_server_port, debug=False, use_reloader=False, threaded=True),
            daemon=True,
        )
        t.start()
        _server_started = True
        return f"http://127.0.0.1:{_server_port}"


