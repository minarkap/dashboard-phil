"""
Procesar CSV de leads de Kajabi para cargar en leads_kajabi
"""
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

from backend.db.config import engine, init_db

logger = logging.getLogger(__name__)


def process_kajabi_leads_csv(csv_content: str) -> Dict[str, Any]:
    """
    Procesa un CSV de leads de Kajabi y los carga en la base de datos.
    
    Args:
        csv_content: Contenido del CSV como string
        
    Returns:
        Dict con estadísticas del procesamiento
    """
    try:
        # Parsear CSV
        df = pd.read_csv(pd.io.common.StringIO(csv_content))
        
        # Asegurar que existe la tabla
        init_db()
        
        # Mapear columnas del CSV a nuestros campos
        # Asumimos que el CSV tiene estas columnas (ajustar según sea necesario):
        # - email, created_at, utm_source, utm_medium, utm_campaign, utm_content, gclid, fbclid
        
        leads_data = []
        processed = 0
        errors = 0
        
        for _, row in df.iterrows():
            try:
                # Extraer email
                email = str(row.get('Email', '')).strip()
                if not email or email == 'nan':
                    continue
                    
                # Extraer fecha de creación
                created_at_str = str(row.get('Created At', '')).strip()
                if created_at_str and created_at_str != 'nan':
                    try:
                        # Intentar diferentes formatos de fecha
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
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
                
                # Extraer UTMs (mapear custom fields del CSV de Kajabi)
                utm_source = str(row.get('utm_source (custom_22)', '')).strip() if pd.notna(row.get('utm_source (custom_22)')) else None
                utm_medium = str(row.get('utm_medium (custom_21)', '')).strip() if pd.notna(row.get('utm_medium (custom_21)')) else None
                utm_campaign = str(row.get('utm_campaign (custom_23)', '')).strip() if pd.notna(row.get('utm_campaign (custom_23)')) else None
                utm_content = str(row.get('utm_content (custom_24)', '')).strip() if pd.notna(row.get('utm_content (custom_24)')) else None
                
                # Extraer IDs de tracking
                gclid = str(row.get('gclid', '')).strip() if pd.notna(row.get('gclid')) else None
                fbclid = str(row.get('fbclid', '')).strip() if pd.notna(row.get('fbclid')) else None
                
                # Determinar plataforma basada en UTMs o IDs
                platform = None
                campaign_id = None
                adset_id = None
                ad_id = None
                
                if gclid:
                    platform = 'google_ads'
                elif fbclid:
                    platform = 'meta'
                elif utm_medium and 'meta-ads' in utm_medium.lower():
                    platform = 'meta'
                elif utm_medium and 'test-ads' in utm_medium.lower():
                    platform = 'google_ads'  # test-ads probablemente sea Google Ads
                elif utm_source and utm_source.lower() in ['youtube', 'google', 'gclid']:
                    platform = 'google_ads'
                elif utm_source and utm_source.lower() in ['facebook', 'meta', 'fb', 'instagram']:
                    platform = 'meta'
                elif utm_medium and ('cpc' in utm_medium.lower() or 'ppc' in utm_medium.lower()):
                    # Si es CPC/PPC, intentar determinar por campaign o source
                    if utm_campaign and ('google' in utm_campaign.lower() or 'ads' in utm_campaign.lower()):
                        platform = 'google_ads'
                    elif utm_campaign and ('facebook' in utm_campaign.lower() or 'meta' in utm_campaign.lower() or 'fb' in utm_campaign.lower()):
                        platform = 'meta'
                    elif utm_source and ('google' in utm_source.lower() or 'gclid' in utm_source.lower()):
                        platform = 'google_ads'
                    elif utm_source and ('facebook' in utm_source.lower() or 'meta' in utm_source.lower() or 'fb' in utm_source.lower()):
                        platform = 'meta'
                    else:
                        # Por defecto a Google Ads si es paid pero no se puede determinar
                        platform = 'google_ads'
                elif utm_source and ('google' in utm_source.lower() or 'gclid' in utm_source.lower()):
                    platform = 'google_ads'
                elif utm_source and ('facebook' in utm_source.lower() or 'meta' in utm_source.lower() or 'fb' in utm_source.lower()):
                    platform = 'meta'
                
                # Extraer IDs de campaña si están disponibles
                campaign_id = str(row.get('campaign_id', '')).strip() if pd.notna(row.get('campaign_id')) else None
                adset_id = str(row.get('adset_id', '')).strip() if pd.notna(row.get('adset_id')) else None
                ad_id = str(row.get('ad_id', '')).strip() if pd.notna(row.get('ad_id')) else None
                
                lead_data = {
                    'email': email,
                    'created_at': created_at,
                    'utm_source': utm_source if utm_source and utm_source != 'nan' else None,
                    'utm_medium': utm_medium if utm_medium and utm_medium != 'nan' else None,
                    'utm_campaign': utm_campaign if utm_campaign and utm_campaign != 'nan' else None,
                    'utm_content': utm_content if utm_content and utm_content != 'nan' else None,
                    'gclid': gclid if gclid and gclid != 'nan' else None,
                    'fbclid': fbclid if fbclid and fbclid != 'nan' else None,
                    'platform': platform,
                    'campaign_id': campaign_id if campaign_id and campaign_id != 'nan' else None,
                    'adset_id': adset_id if adset_id and adset_id != 'nan' else None,
                    'ad_id': ad_id if ad_id and ad_id != 'nan' else None,
                }
                
                leads_data.append(lead_data)
                processed += 1
                
            except Exception as e:
                logger.error(f"Error procesando fila {processed}: {e}")
                errors += 1
                continue
        
        if not leads_data:
            return {
                'success': False,
                'message': 'No se encontraron leads válidos en el CSV',
                'processed': 0,
                'errors': errors
            }
        
        # Insertar en base de datos
        with engine.begin() as conn:
            from sqlalchemy import text
            # Insertar directamente (sin ON CONFLICT por ahora)
            insert_sql = text("""
            INSERT INTO leads_kajabi (
                email, created_at, utm_source, utm_medium, utm_campaign, utm_content,
                gclid, fbclid, platform, campaign_id, adset_id, ad_id
            ) VALUES (
                :email, :created_at, :utm_source, :utm_medium, :utm_campaign, :utm_content,
                :gclid, :fbclid, :platform, :campaign_id, :adset_id, :ad_id
            )
            """)
            
            # Insertar uno por uno
            for lead_data in leads_data:
                try:
                    conn.execute(insert_sql, lead_data)
                except Exception as e:
                    # Si hay duplicados, continuar con el siguiente
                    if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                        continue
                    else:
                        raise e
        
        return {
            'success': True,
            'message': f'CSV procesado correctamente: {processed} leads cargados',
            'processed': processed,
            'errors': errors
        }
        
    except Exception as e:
        logger.error(f"Error procesando CSV de leads: {e}")
        return {
            'success': False,
            'message': f'Error procesando CSV: {str(e)}',
            'processed': 0,
            'errors': 1
        }
