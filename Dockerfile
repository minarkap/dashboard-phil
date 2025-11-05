FROM python:3.11-slim

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

# Copiar el resto del código
COPY . /app/

# Crear directorio para logs si es necesario
RUN mkdir -p /app/logs

# Exponer puerto (Railway usa PORT, default 8501 para Streamlit)
EXPOSE 8501

# Variable de entorno para el puerto (Railway la proporciona)
ENV PORT=8501

# Script de inicio que usa PORT si está disponible
CMD sh -c "streamlit run streamlit_app/app.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true"

