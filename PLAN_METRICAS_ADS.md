# Plan de Integración de Métricas: GA4, Google Ads y Meta Ads

## Objetivo
Crear un dashboard unificado que muestre métricas relevantes de las tres fuentes, con revenue total de GA4 como fuente de verdad, y vistas alternativas (tablas vs gráficos).

---

## 1. ANÁLISIS DE FUENTES DE DATOS

### 1.1 Google Analytics 4 (GA4) - Fuente de Verdad para Revenue
**Datos disponibles:**
- ✅ Eventos `purchase` por día, item, source/medium/campaign
- ✅ Revenue (`purchaseRevenue`) por transacción
- ✅ Métricas: `eventCount` (purchases), `itemPurchaseQuantity`
- ⚠️ No guardamos purchases/revenue en BD actualmente (se calculan on-the-fly)

**Lo que necesitamos:**
- Tabla `ga4_purchases_daily` para guardar purchases y revenue segmentados
- Sincronización periódica desde GA4 Data API
- Segmentación por: date, source, medium, campaign, item_name (opcional)

### 1.2 Google Ads
**Datos disponibles:**
- ✅ Costos (`ad_costs_daily`)
- ✅ Conversiones (`google_ads_insights_daily`) - 2840 registros históricos
- ⚠️ Revenue viene de conversiones_value (puede no cuadrar con GA4)

**Lo que necesitamos:**
- Usar `google_ads_insights_daily` para purchases/revenue por campaña/adset/ad
- Revenue de Google Ads solo para contexto interno (no sumar con Meta)

### 1.3 Meta Ads
**Datos disponibles:**
- ✅ Costos (`ad_costs_daily`)
- ✅ Purchases (`meta_insights_daily`) - 7834 registros históricos
- ✅ Revenue (`purchase_value`) por campaña/adset/ad

**Lo que necesitamos:**
- Usar `meta_insights_daily` para purchases/revenue
- Revenue de Meta solo para contexto interno (no sumar con Google Ads)

---

## 2. MODELO DE DATOS

### 2.1 Nueva Tabla: `ga4_purchases_daily`
```sql
CREATE TABLE ga4_purchases_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    source VARCHAR(100),
    medium VARCHAR(100),
    campaign VARCHAR(200),
    item_name VARCHAR(500),  -- Opcional, para granularidad
    purchases INTEGER DEFAULT 0,
    revenue_eur NUMERIC(18, 4) DEFAULT 0.0,
    platform_detected VARCHAR(20),  -- 'google_ads', 'meta', 'organic', etc.
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(date, source, medium, campaign, COALESCE(item_name, ''))
);
```

**Propósito:**
- Guardar purchases/revenue de GA4 como fuente de verdad
- Permitir segmentación por source/medium/campaign
- Detectar automáticamente plataforma (google_ads si medium='cpc' y source='google', etc.)

### 2.2 Tablas Existentes (ya tenemos):
- ✅ `meta_insights_daily` - Purchases y revenue de Meta
- ✅ `google_ads_insights_daily` - Conversiones y revenue de Google Ads
- ✅ `ad_costs_daily` - Costos de ambas plataformas
- ✅ `ga_sessions_daily` - Sesiones y conversiones genéricas de GA4

---

## 3. SINCRONIZACIÓN DE DATOS

### 3.1 GA4 Purchases Sincronización
**Función:** `sync_ga4_purchases(start_date, end_date)`

**Lógica:**
1. Consultar GA4 Data API para eventos `purchase`
2. Dimensiones: `date`, `sessionSource`, `sessionMedium`, `sessionCampaignName`, `itemName` (opcional)
3. Métricas: `eventCount`, `purchaseRevenue`
4. Detectar plataforma automáticamente:
   - `google_ads`: source='google' AND medium IN ('cpc', 'ppc')
   - `meta`: source IN ('facebook', 'instagram') AND medium IN ('cpc', 'ppc')
   - `organic`: medium NOT IN ('cpc', 'ppc')
5. Guardar en `ga4_purchases_daily` con upsert

### 3.2 Google Ads Insights
**Estado:** ✅ Ya sincronizado (2840 registros)
**Frecuencia:** Mantener sincronización diaria

### 3.3 Meta Ads Insights
**Estado:** ✅ Ya sincronizado (7834 registros históricos)
**Frecuencia:** Mantener sincronización diaria

---

## 4. MÉTRICAS Y SEGMENTACIÓN

### 4.1 Métricas Globales (Revenue Total)
**Fuente:** GA4 (`ga4_purchases_daily`)
- Total Revenue (EUR)
- Total Purchases
- ROAS Global = Revenue Total / Costos Totales (Ads)
- CPA Global = Costos Totales / Purchases

### 4.2 Métricas por Plataforma

**Google Ads:**
- Costos (`ad_costs_daily`)
- Purchases (`google_ads_insights_daily.conversions`)
- Revenue (`google_ads_insights_daily.conversions_value`)
- Leads (`leads_kajabi` filtrados)
- ROAS = Revenue / Costos
- CPA = Costos / Purchases
- CPL = Costos / Leads
- CR = Purchases / Leads

**Meta Ads:**
- Costos (`ad_costs_daily`)
- Purchases (`meta_insights_daily.purchases`)
- Revenue (`meta_insights_daily.purchase_value`)
- Leads (`leads_kajabi` filtrados)
- ROAS = Revenue / Costos
- CPA = Costos / Purchases
- CPL = Costos / Leads
- CR = Purchases / Leads

### 4.3 Segmentación
**Niveles:**
1. Global (todas las plataformas)
2. Por Plataforma (Google Ads, Meta Ads, Organic)
3. Por Campaña
4. Por Adset (solo Ads)
5. Por Anuncio (solo Ads)

**Dimensiones:**
- Fecha (día, semana, mes)
- Plataforma
- Campaña
- Adset
- Anuncio
- Item/Producto (si aplica)

---

## 5. VISTAS DEL DASHBOARD

### 5.1 Toggle: Tablas vs Gráficos

**Vista Tablas:**
- Tablas detalladas con todas las métricas
- Filtros y ordenación
- Descarga CSV

**Vista Gráficos:**
- Gráficos interactivos (Plotly)
- Líneas temporales (revenue, costos, purchases)
- Gráficos de barras (por campaña, plataforma)
- Gráficos de dispersión (ROAS vs CPA)
- Heatmaps (performance por día/plataforma)

### 5.2 Secciones del Dashboard

**1. Resumen Global**
- KPI Cards: Revenue Total, Purchases Totales, ROAS Global, CPA Global
- Gráfico: Revenue y Costos por día (líneas superpuestas)

**2. Por Plataforma**
- Google Ads: Métricas completas + Tabla/Gráfico
- Meta Ads: Métricas completas + Tabla/Gráfico
- Organic (GA4): Revenue y Purchases sin costos

**3. Detalle por Segmento**
- Tabla/Gráfico por Campaña
- Tabla/Gráfico por Adset
- Tabla/Gráfico por Anuncio

---

## 6. IMPLEMENTACIÓN TÉCNICA

### Fase 1: Modelo de Datos
1. Crear modelo `GA4PurchasesDaily` en `backend/db/models.py`
2. Migrar/crear tabla `ga4_purchases_daily`
3. Añadir índices para consultas rápidas

### Fase 2: Sincronización
1. Crear `backend/etl/ga4_purchases_sync.py`
2. Función `sync_ga4_purchases(start_date, end_date)`
3. Integrar en botones de sincronización en `tabs_ingest.py`

### Fase 3: Carga de Datos
1. Función `load_ga4_purchases()` en `streamlit_app/data.py`
2. Función `load_ads_metrics_unified()` para datos combinados
3. Función `load_metrics_by_segment()` para segmentación

### Fase 4: Dashboard UI
1. Añadir toggle Tablas/Gráficos en `tabs_ads.py`
2. Crear función `render_table_view()` y `render_graph_view()`
3. Integrar Plotly para gráficos interactivos
4. KPI cards con métricas globales

### Fase 5: Validación y QA
1. Reconciliar revenue: GA4 vs Ads (esperar diferencias)
2. Validar segmentación por campaña/adset/ad
3. Testear rendimiento con datos históricos

---

## 7. ORDEN DE EJECUCIÓN

**Paso 1:** Crear tabla y modelo GA4 Purchases
**Paso 2:** Implementar sincronización GA4 Purchases
**Paso 3:** Crear funciones de carga unificadas
**Paso 4:** Añadir toggle y vistas al dashboard
**Paso 5:** Sincronizar datos históricos de GA4
**Paso 6:** Validar y ajustar

---

## 8. CONSIDERACIONES

**Revenue Total:**
- Usar GA4 como fuente única de verdad
- No sumar revenue de Google Ads + Meta Ads (doble atribución)
- Mostrar revenue de Ads solo para contexto interno de cada plataforma

**Atribución:**
- GA4: atribución por defecto (Last Click o configurada)
- Google Ads: atribución propia (data-driven o última interacción)
- Meta Ads: atribución propia (7d click + 1d view)

**Desfases:**
- Las plataformas pueden tardar en reportar conversiones
- Planificar re-sincronización de últimos 7-28 días


