# Configuración del Webhook de Kajabi

## 📋 Resumen
Este webhook permite recibir leads de Kajabi en tiempo real y almacenarlos automáticamente en la base de datos con sus UTMs correspondientes.

## 🚀 Iniciar el Webhook

### Opción 1: Desde Streamlit
1. Ve a la pestaña **"Ingesta"**
2. Busca la sección **"Webhook de Kajabi"**
3. Haz clic en **"Iniciar Webhook Kajabi"**
4. El webhook estará disponible en: `http://localhost:5001/webhook/kajabi/contact`

### Opción 2: Desde terminal
```bash
cd /Users/JoseSanchis/Projects/phil_hugo/dashboard
source venv/bin/activate
python3 start_webhook.py
```

## 🔧 Configurar en Kajabi

### 1. Acceder a la configuración de webhooks
1. Inicia sesión en tu cuenta de Kajabi
2. Ve a **Settings** → **Integrations** → **Webhooks**
3. Haz clic en **"Create Webhook"**

### 2. Configurar el webhook
- **Webhook URL**: `https://tu-dominio.com/webhook/kajabi/contact`
  - Si estás en local: `http://localhost:5001/webhook/kajabi/contact`
  - Para producción: usa tu dominio público con ngrok o similar
- **Events**: Selecciona **"Contact Created"** y **"Contact Updated"**
- **Method**: POST
- **Content Type**: application/json

### 3. Formato de datos esperado
El webhook espera recibir datos en este formato:

```json
{
  "email": "user@example.com",
  "created_at": "2023-01-01 12:00:00 +0000",
  "custom_fields": {
    "utm_source": "google",
    "utm_medium": "cpc", 
    "utm_campaign": "campaign_name",
    "utm_content": "ad_content"
  },
  "gclid": "optional_google_click_id",
  "fbclid": "optional_facebook_click_id",
  "campaign_id": "optional_campaign_id",
  "adset_id": "optional_adset_id", 
  "ad_id": "optional_ad_id"
}
```

## 🧪 Probar el Webhook

### Verificar estado
```bash
curl http://localhost:5001/webhook/kajabi/contact
```

### Simular webhook
```bash
curl -X POST http://localhost:5001/webhook/kajabi/contact \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "created_at": "2023-01-01 12:00:00 +0000",
    "custom_fields": {
      "utm_source": "google",
      "utm_medium": "cpc",
      "utm_campaign": "test_campaign"
    }
  }'
```

## 📊 Verificar datos

Una vez configurado, los leads se almacenarán automáticamente en la tabla `leads_kajabi`. Puedes verificar los datos:

1. Ve a la pestaña **"Ads"** en Streamlit
2. Los leads aparecerán en las métricas de ads
3. O consulta directamente la base de datos:

```sql
SELECT COUNT(*) FROM leads_kajabi;
SELECT platform, COUNT(*) FROM leads_kajabi GROUP BY platform;
```

## 🔍 Logs y Debugging

Los logs del webhook se muestran en la consola donde lo ejecutes. Para debugging:

1. Revisa los logs en la consola
2. Usa el botón **"Verificar Webhook"** en Streamlit
3. Comprueba que Kajabi esté enviando los datos correctamente

## 🚨 Solución de problemas

### Webhook no responde
- Verifica que el puerto 5001 esté libre
- Comprueba que el webhook esté ejecutándose
- Revisa los logs de error

### Datos no se almacenan
- Verifica que la base de datos esté accesible
- Comprueba el formato de los datos de Kajabi
- Revisa los logs de error en la consola

### UTMs no se detectan
- Asegúrate de que Kajabi esté enviando los custom fields correctamente
- Verifica que los nombres de los campos coincidan con el formato esperado

