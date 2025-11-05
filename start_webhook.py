#!/usr/bin/env python3
"""
Script para iniciar el webhook de Kajabi
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backend.webhooks.kajabi_webhook import app

if __name__ == '__main__':
    port = int(os.getenv('PORT', '5001'))
    print(f"Iniciando webhook de Kajabi en puerto {port}...")
    print(f"Endpoint: http://localhost:{port}/webhook/kajabi/contact")
    print(f"Estado: http://localhost:{port}/webhook/kajabi/contact (GET)")
    print("Presiona Ctrl+C para detener")
    app.run(host='0.0.0.0', port=port, debug=False)

