"""
Script para sincronizar todos los datos históricos de Meta Ads en lotes pequeños
para evitar límites de rate de la API.
"""
import sys
import os
import time
from datetime import date, timedelta

# Añadir el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.etl.meta_client import sync_meta_insights
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def sync_meta_alltime():
    """Sincroniza todos los datos históricos de Meta Ads mes a mes"""
    start_date = date(2025, 1, 1)
    end_date = date.today()
    
    print(f'\n=== SINCRONIZANDO META ADS HISTÓRICOS ===')
    print(f'Desde: {start_date}')
    print(f'Hasta: {end_date}\n')
    
    current = start_date
    month_count = 0
    
    while current <= end_date:
        # Calcular fin del mes
        if current.month == 12:
            month_end = date(current.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(current.year, current.month + 1, 1) - timedelta(days=1)
        
        month_end = min(month_end, end_date)
        month_count += 1
        
        print(f'Mes {month_count}: {current.strftime("%Y-%m")} ({current} a {month_end})...', end=' ')
        
        try:
            sync_meta_insights(current, month_end)
            print('✓')
            time.sleep(2)  # Pausa entre meses para evitar rate limits
        except Exception as e:
            error_str = str(e)
            if 'rate limit' in error_str.lower() or '1504022' in error_str:
                print(f'⚠️ Rate limit alcanzado, esperando 60 segundos...')
                time.sleep(60)
                # Reintentar
                try:
                    sync_meta_insights(current, month_end)
                    print('  ✓ (después de esperar)')
                except Exception as e2:
                    print(f'  ✗ Error persistente: {str(e2)[:150]}')
            elif 'timeout' in error_str.lower() or '1504018' in error_str:
                print(f'⚠️ Timeout, intentando con semanas...')
                # Intentar semana a semana
                week_start = current
                while week_start <= month_end:
                    week_end = min(week_start + timedelta(days=6), month_end)
                    try:
                        sync_meta_insights(week_start, week_end)
                        time.sleep(1)
                    except Exception as e3:
                        logger.warning(f'  Error en semana {week_start} a {week_end}: {str(e3)[:100]}')
                    week_start = week_end + timedelta(days=1)
                print('  ✓ (por semanas)')
            else:
                print(f'✗ {error_str[:150]}')
        
        # Siguiente mes
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    
    print('\n=== Sincronización histórica completada ===')

if __name__ == "__main__":
    sync_meta_alltime()

