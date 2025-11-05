FROM python:3.11-slim
LABEL build.ts="2025-11-05T16:55:00Z"

# Establecer directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivos de requirements
COPY requirements.txt /app/
COPY backend/requirements.txt /app/backend-requirements.txt

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r backend-requirements.txt

# Añade un LABEL para invalidar la caché de Railway y forzar un rebuild fresco
LABEL build.ts="2025-11-05T17:45:00Z"

# --- PASO DE FUERZA BRUTA ---
# Copiar explícitamente backend/db para anular cualquier posible problema de .dockerignore
COPY backend/db /app/backend/db

# Copiar el resto del código
COPY . /app/

# --- PASO DE DEPURACIÓN ---
# Listar el contenido de /app para verificar que todos los archivos se copiaron correctamente.
# Busca "backend/db/config.py" en los logs del build de Railway.
RUN ls -laR /app

# Crear directorio para logs si es necesario
RUN mkdir -p /app/logs

# Exponer puerto (Railway usa PORT, default 8501 para Streamlit)
EXPOSE 8501

# Variables de entorno
# - PORT: Railway la proporciona
# - PYTHONPATH: garantiza que /app esté en sys.path para imports como 'backend.*'
ENV PORT=8501 \
    PYTHONPATH=/app

# Script de inicio que usa PORT si está disponible
CMD sh -c "streamlit run streamlit_app/app.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true"

