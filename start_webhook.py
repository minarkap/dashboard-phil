#!/usr/bin/env python3
"""
Script para iniciar el webhook de Kajabi
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backend.webhooks.kajabi_webhook import app

if __name__ == '__main__':
    print("Iniciando webhook de Kajabi en puerto 5001...")
    print("Endpoint: http://localhost:5001/webhook/kajabi/contact")
    print("Estado: http://localhost:5001/webhook/kajabi/contact (GET)")
    print("Presiona Ctrl+C para detener")
    app.run(host='0.0.0.0', port=5001, debug=True)

