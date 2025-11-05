"""
Webhook handler para leads de Kajabi
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify

from backend.db.config import engine, init_db
from backend.etl.kajabi_csv_leads import process_kajabi_leads_csv

logger = logging.getLogger(__name__)

app = Flask(__name__)

def process_kajabi_webhook_lead(webhook_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Procesa un lead individual desde el webhook de Kajabi.
    
    Args:
        webhook_data: Datos del webhook de Kajabi
        
    Returns:
        Dict con resultado del procesamiento
    """
    try:
        # Asegurar que existe la tabla
        init_db()
        
        # Extraer datos del webhook
        email = webhook_data.get('email', '').strip()
        if not email:
            return {'success': False, 'message': 'Email requerido'}
        
        # Extraer fecha de creación
        created_at_str = webhook_data.get('created_at', '')
        if created_at_str:
            try:
                # Intentar diferentes formatos de fecha
                for fmt in ['%Y-%m-%d %H:%M:%S %z', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                    try:
                        created_at = datetime.strptime(created_at_str, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    created_at = datetime.now()
            except:
                created_at = datetime.now()
        else:
            created_at = datetime.now()
        
        # Extraer UTMs de custom fields
        custom_fields = webhook_data.get('custom_fields', {})
        utm_source = custom_fields.get('utm_source', '').strip() or None
        utm_medium = custom_fields.get('utm_medium', '').strip() or None
        utm_campaign = custom_fields.get('utm_campaign', '').strip() or None
        utm_content = custom_fields.get('utm_content', '').strip() or None
        
        # Extraer IDs de tracking
        gclid = webhook_data.get('gclid', '').strip() or None
        fbclid = webhook_data.get('fbclid', '').strip() or None
        
        # Determinar plataforma basada en UTMs o IDs
        platform = None
        if gclid:
            platform = 'google_ads'
        elif fbclid:
            platform = 'meta'
        elif utm_source and 'google' in utm_source.lower():
            platform = 'google_ads'
        elif utm_source and ('facebook' in utm_source.lower() or 'meta' in utm_source.lower()):
            platform = 'meta'
        
        # Extraer IDs de campaña si están disponibles
        campaign_id = webhook_data.get('campaign_id', '').strip() or None
        adset_id = webhook_data.get('adset_id', '').strip() or None
        ad_id = webhook_data.get('ad_id', '').strip() or None
        
        lead_data = {
            'email': email,
            'created_at': created_at,
            'utm_source': utm_source,
            'utm_medium': utm_medium,
            'utm_campaign': utm_campaign,
            'utm_content': utm_content,
            'gclid': gclid,
            'fbclid': fbclid,
            'platform': platform,
            'campaign_id': campaign_id,
            'adset_id': adset_id,
            'ad_id': ad_id,
        }
        
        # Insertar en base de datos
        with engine.begin() as conn:
            from sqlalchemy import text
            insert_sql = text("""
            INSERT INTO leads_kajabi (
                email, created_at, utm_source, utm_medium, utm_campaign, utm_content,
                gclid, fbclid, platform, campaign_id, adset_id, ad_id
            ) VALUES (
                :email, :created_at, :utm_source, :utm_medium, :utm_campaign, :utm_content,
                :gclid, :fbclid, :platform, :campaign_id, :adset_id, :ad_id
            ) ON CONFLICT (email) DO UPDATE SET
                created_at = EXCLUDED.created_at,
                utm_source = EXCLUDED.utm_source,
                utm_medium = EXCLUDED.utm_medium,
                utm_campaign = EXCLUDED.utm_campaign,
                utm_content = EXCLUDED.utm_content,
                gclid = EXCLUDED.gclid,
                fbclid = EXCLUDED.fbclid,
                platform = EXCLUDED.platform,
                campaign_id = EXCLUDED.campaign_id,
                adset_id = EXCLUDED.adset_id,
                ad_id = EXCLUDED.ad_id
            """)
            
            conn.execute(insert_sql, lead_data)
        
        return {
            'success': True,
            'message': f'Lead procesado: {email}',
            'email': email,
            'platform': platform
        }
        
    except Exception as e:
        logger.error(f"Error procesando webhook lead: {e}")
        return {
            'success': False,
            'message': f'Error procesando lead: {str(e)}',
            'email': email
        }

@app.route('/webhook/kajabi/contact', methods=['POST'])
def handle_kajabi_contact_webhook():
    """
    Endpoint para recibir webhooks de contactos de Kajabi.
    
    Formato esperado:
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
    """
    try:
        # Verificar que es POST
        if request.method != 'POST':
            return jsonify({'error': 'Method not allowed'}), 405
        
        # Obtener datos del webhook
        webhook_data = request.get_json()
        if not webhook_data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # Procesar el lead
        result = process_kajabi_webhook_lead(webhook_data)
        
        if result['success']:
            logger.info(f"Webhook lead procesado: {result['email']}")
            return jsonify(result), 200
        else:
            logger.error(f"Error procesando webhook lead: {result['message']}")
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error en webhook: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/webhook/kajabi/contact', methods=['GET'])
def webhook_status():
    """Endpoint de estado para verificar que el webhook está funcionando."""
    return jsonify({
        'status': 'ok',
        'message': 'Kajabi contact webhook is running',
        'timestamp': datetime.now().isoformat()
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)

