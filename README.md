# Dashboard de Ventas - Phil Hugo

Dashboard de análisis de ventas integrado con múltiples plataformas: Stripe, Hotmart, Kajabi, Google Ads, Meta Ads, Google Analytics 4 y Google Sheets.

## 🏗️ Arquitectura

- **Frontend**: Streamlit (interfaz web interactiva)
- **Backend**: Python 3.11+ con SQLAlchemy
- **Base de datos**: PostgreSQL 16
- **ETL**: Sincronización automática con múltiples APIs
- **Despliegue**: Docker + Railway

## 📋 Requisitos

- Docker y Docker Compose (para desarrollo local)
- Python 3.11+ (opcional, solo si no usas Docker)
- Cuentas y credenciales de las plataformas que quieras integrar

## 🚀 Inicio Rápido

### Desarrollo Local con Docker

1. **Clonar el repositorio**:
```bash
git clone git@github.com:minarkap/dashboard-phil.git
cd dashboard-phil
```

2. **Configurar variables de entorno**:
```bash
cp .env.example .env
# Edita .env y completa las variables necesarias
```

3. **Levantar servicios con Docker Compose**:
```bash
docker compose up -d
```

Esto iniciará:
- PostgreSQL en el puerto 5432
- Dashboard Streamlit en http://localhost:8501

4. **Acceder al dashboard**:
Abre tu navegador en `http://localhost:8501`

### Primera Sincronización de Datos

Una vez que los servicios estén corriendo, ejecuta la sincronización inicial:

```bash
# Si usas Docker
docker compose exec app python -m backend.run_sync

# O si usas Python local
python -m backend.run_sync
```

Esto sincronizará datos de todas las plataformas configuradas (Stripe, Hotmart, Kajabi, Google Ads, Meta Ads, GA4).

## 🔧 Configuración de Variables de Entorno

Copia `.env.example` a `.env` y completa las variables según las plataformas que uses:

### Variables Obligatorias

- `DATABASE_URL`: URL de conexión a PostgreSQL (Railway la proporciona automáticamente)
- `DASHBOARD_PASSWORD` o `APP_PASSWORD`: Contraseña para proteger el acceso al dashboard

### Variables por Plataforma

Consulta `.env.example` para ver todas las variables disponibles. Las principales son:

- **Stripe**: `STRIPE_API_KEY`
- **Google Ads**: `GOOGLE_ADS_DEVELOPER_TOKEN`, `GOOGLE_ADS_CUSTOMER_ID`, credenciales OAuth
- **GA4**: `GA4_PROPERTY_ID`, credenciales OAuth
- **Meta Ads**: `META_ACCESS_TOKEN`, `META_AD_ACCOUNT_ID`
- **Kajabi**: `KAJABI_CLIENT_ID`, `KAJABI_CLIENT_SECRET`
- **Hotmart**: `HOTMART_ACCESS_TOKEN` o `HOTMART_CLIENT_ID` + `HOTMART_CLIENT_SECRET`
- **Google Sheets**: `GOOGLE_SHEETS_CREDENTIALS_JSON` o credenciales OAuth

## 🚂 Despliegue en Railway

### Paso 1: Preparar el Repositorio

1. Asegúrate de que todos los archivos estén en GitHub:
   - `Dockerfile`
   - `docker-compose.yml` (opcional, Railway usará el Dockerfile)
   - `.env.example`
   - `requirements.txt` y `backend/requirements.txt`

2. Verifica que `.env` esté en `.gitignore` (no debe subirse a GitHub)

### Paso 2: Crear Proyecto en Railway

1. Ve a [Railway](https://railway.app) e inicia sesión
2. Haz clic en "New Project"
3. Selecciona "Deploy from GitHub repo"
4. Conecta tu repositorio: `minarkap/dashboard-phil`
5. Railway detectará automáticamente el `Dockerfile`

### Paso 3: Configurar Base de Datos PostgreSQL

1. En tu proyecto de Railway, haz clic en "+ New"
2. Selecciona "Database" → "Add PostgreSQL"
3. Railway creará automáticamente la base de datos y la variable `DATABASE_URL`

### Paso 4: Configurar Variables de Entorno

1. En tu servicio de Railway, ve a la pestaña "Variables"
2. Haz clic en "Raw Editor" o "Add Variable"
3. Copia todas las variables de `.env.example` que necesites
4. Pega los valores reales (no uses valores de ejemplo)

**Importante**: 
- Railway proporciona automáticamente `PORT` y `DATABASE_URL`
- No necesitas configurar `PORT` manualmente
- Asegúrate de que `DATABASE_URL` esté configurada (Railway la crea automáticamente)

### Paso 5: Desplegar

1. Railway detectará automáticamente el `Dockerfile` y comenzará a construir
2. El despliegue se completará automáticamente
3. Haz clic en "Settings" → "Generate Domain" para obtener la URL pública

### Paso 6: Verificar el Despliegue

1. Abre la URL generada por Railway
2. Deberías ver el dashboard de Streamlit
3. Si configuraste `DASHBOARD_PASSWORD`, se te pedirá la contraseña

### Paso 7: Sincronización Inicial (Railway)

Para ejecutar la sincronización inicial en Railway:

1. Ve a tu servicio en Railway
2. Haz clic en "Deployments" → selecciona el deployment más reciente
3. Haz clic en "View Logs"
4. Abre la terminal o ejecuta:
```bash
railway run python -m backend.run_sync
```

O desde la CLI de Railway:
```bash
railway run python -m backend.run_sync
```

## 📁 Estructura del Proyecto

```
dashboard/
├── backend/
│   ├── db/              # Modelos y configuración de base de datos
│   │   ├── config.py    # Configuración SQLAlchemy
│   │   └── models.py    # Modelos de datos
│   ├── etl/             # Conectores y sincronizadores
│   │   ├── stripe_sync.py
│   │   ├── hotmart_sync.py
│   │   ├── kajabi_sync.py
│   │   ├── google_ads_sync.py
│   │   ├── meta_client.py
│   │   └── ga4_client.py
│   ├── webhooks/        # Endpoints para webhooks
│   └── run_sync.py      # Script principal de sincronización
├── streamlit_app/
│   ├── app.py           # Aplicación principal Streamlit
│   ├── tabs_*.py        # Pestañas del dashboard
│   ├── data.py          # Funciones de carga de datos
│   └── utils.py         # Utilidades
├── Dockerfile           # Configuración Docker
├── docker-compose.yml   # Orquestación local
├── .env.example         # Plantilla de variables de entorno
└── requirements.txt     # Dependencias Python
```

## 🔄 Sincronización de Datos

El sistema sincroniza datos de múltiples fuentes:

- **Stripe**: Pagos, reembolsos, suscripciones
- **Hotmart**: Transacciones de productos
- **Kajabi**: Órdenes, suscripciones, leads
- **Google Ads**: Costes por campaña/anuncio
- **Meta Ads**: Costes por campaña/anuncio
- **Google Analytics 4**: Sesiones, conversiones, eventos

### Ejecutar Sincronización Manual

```bash
# Sincronizar todas las fuentes
python -m backend.run_sync

# Sincronizar solo Hotmart
python -m backend.run_hotmart_sync
```

### Sincronización Automática

Puedes configurar un cron job o scheduler (como APScheduler) para ejecutar sincronizaciones periódicas.

## 🐛 Troubleshooting

### Error de conexión a la base de datos

- Verifica que `DATABASE_URL` esté correctamente configurada
- En Railway, asegúrate de que el servicio PostgreSQL esté corriendo
- Verifica que la base de datos esté en el mismo proyecto de Railway

### Dashboard no carga

- Revisa los logs en Railway: "View Logs"
- Verifica que todas las variables de entorno estén configuradas
- Asegúrate de que el puerto esté correctamente expuesto

### Error en sincronización

- Verifica las credenciales de la plataforma específica
- Revisa los logs para ver el error detallado
- Algunas APIs requieren aprobación previa (ej: Google Ads Developer Token)

### Variables de entorno no se cargan

- En Railway, verifica que las variables estén en "Variables" del servicio
- Asegúrate de que no haya espacios extra en los valores
- Para valores JSON (como `GOOGLE_SHEETS_CREDENTIALS_JSON`), escapa las comillas correctamente

## 📝 Desarrollo Local sin Docker

Si prefieres no usar Docker:

1. **Instala PostgreSQL** localmente o usa un servicio remoto
2. **Crea un entorno virtual**:
```bash
python3 -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate
```

3. **Instala dependencias**:
```bash
pip install -r requirements.txt
pip install -r backend/requirements.txt
```

4. **Configura `.env`** con tu `DATABASE_URL` local

5. **Ejecuta Streamlit**:
```bash
streamlit run streamlit_app/app.py
```

## 🔐 Seguridad

- **Nunca subas `.env` a GitHub** (está en `.gitignore`)
- Usa variables de entorno en Railway para datos sensibles
- Configura `DASHBOARD_PASSWORD` en producción
- Rota las credenciales periódicamente
- Usa tokens de acceso con permisos mínimos necesarios

## 📚 Documentación Adicional

- [WEBHOOK_SETUP.md](./WEBHOOK_SETUP.md) - Configuración de webhooks de Kajabi
- [PLAN_METRICAS_ADS.md](./PLAN_METRICAS_ADS.md) - Plan de métricas de ads

## 🤝 Contribuir

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit tus cambios (`git commit -m 'Añade nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Abre un Pull Request

## 📄 Licencia

Este proyecto es privado y confidencial.

## 🆘 Soporte

Para problemas o preguntas, abre un issue en GitHub o contacta al equipo de desarrollo.
