import requests
import json
import logging
from typing import Optional, Tuple

logger = logging.getLogger("polymarket_arb.market_scanner")

def get_current_5m_market() -> Tuple[Optional[str], str]:
    """
    Escanea la Gamma API de Polymarket en busca del evento activo de BTC a 5 minutos.
    Devuelve (TokenID, Nombre del Mercado).
    Si no encuentra ninguno, devuelve (None, "").
    """
    url = "https://gamma-api.polymarket.com/events?active=true&closed=false"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logger.error(f"Error accediendo a Gamma API: {e}")
        return None, ""

    for event in data:
        title = event.get('title', '').upper()
        # Buscar variaciones de mercados BTC de 5 minutos
        if 'BTC' in title and ('5 M' in title or '5M' in title or '5 MIN' in title):
            markets = event.get('markets', [])
            for market in markets:
                if not market.get('active') or market.get('closed'):
                    continue
                
                clob_ids = market.get('clobTokenIds', [])
                if clob_ids:
                    try:
                        if isinstance(clob_ids, str):
                            clob_ids = json.loads(clob_ids)
                        if isinstance(clob_ids, list) and len(clob_ids) > 0:
                            token_id = clob_ids[0]
                            market_name = market.get('question', title)
                            return token_id, market_name
                    except Exception as e:
                        logger.error(f"Error parseando clobTokenIds: {e}")
                        continue
    
    return None, ""
