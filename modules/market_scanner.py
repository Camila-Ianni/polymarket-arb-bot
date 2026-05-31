import aiohttp
import asyncio
import logging
import json
import re
from typing import Optional, Tuple, Dict, Any
from datetime import datetime

logger = logging.getLogger("polymarket_arb.market_scanner")

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/"
}

BTC_5MIN_SLUG_RE = re.compile(r'btc.*?5m|btc.*?5-min|bitcoin.*?5m|bitcoin.*?5-min', re.IGNORECASE)

def _is_btc_5min(title: str, slug: str = "") -> bool:
    title_upper = title.upper()
    if 'BTC' in title_upper or 'BITCOIN' in title_upper:
        if '5 M' in title_upper or '5M' in title_upper or '5 MIN' in title_upper:
            return True
    if slug and BTC_5MIN_SLUG_RE.search(slug):
        return True
    return False

def _extract_market_fields(market: Dict[str, Any], title: str) -> Tuple[Optional[str], str]:
    if not market.get('active') or market.get('closed'):
        return None, ""
    clob_ids = market.get('clobTokenIds', [])
    if clob_ids:
        try:
            if isinstance(clob_ids, str):
                clob_ids = json.loads(clob_ids)
            if isinstance(clob_ids, list) and len(clob_ids) > 0:
                return clob_ids[0], market.get('question', title)
        except Exception as e:
            logger.warning(f"Error parseando clobTokenIds: {e}")
    return None, ""

def _extract_clob_market(market: Dict[str, Any]) -> Tuple[Optional[str], str]:
    if not market.get('active') or market.get('closed'):
        return None, ""
    question = market.get('question', '')
    if _is_btc_5min(question):
        tokens = market.get('tokens', [])
        if tokens:
            tid = tokens[0].get('token_id')
            if tid:
                return tid, question
    return None, ""

def _parse_ts(ts_str: str) -> Optional[datetime]:
    try:
        # Polymarket suele devolver ISO format
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return None

class MarketScanner:
    def __init__(self, dashboard=None):
        self.gamma_url = "https://gamma-api.polymarket.com/events?active=true&closed=false"
        self.clob_url = "https://clob.polymarket.com/markets"
        self.dashboard = dashboard

    async def _fetch_gamma(self) -> Tuple[Optional[str], str]:
        try:
            async with aiohttp.ClientSession(headers=BROWSER_HEADERS) as session:
                async with session.get(self.gamma_url, timeout=5.0) as response:
                    if response.status == 403:
                        logger.warning("Gamma API retornó 403 (WAF Cloudflare bloqueado).")
                        return None, ""
                    response.raise_for_status()
                    data = await response.json()
                    
                    for event in data:
                        title = event.get('title', '')
                        slug = event.get('slug', '')
                        if _is_btc_5min(title, slug):
                            markets = event.get('markets', [])
                            for market in markets:
                                tid, name = _extract_market_fields(market, title)
                                if tid:
                                    logger.info(f"Mercado encontrado vía Gamma: {name}")
                                    return tid, name
        except Exception as e:
            logger.warning(f"Error en _fetch_gamma: {e}")
        return None, ""

    async def _fetch_clob(self) -> Tuple[Optional[str], str]:
        try:
            async with aiohttp.ClientSession(headers=BROWSER_HEADERS) as session:
                async with session.get(self.clob_url, timeout=5.0) as response:
                    response.raise_for_status()
                    data = await response.json()
                    markets = data.get('data', [])
                    for market in markets:
                        tid, name = _extract_clob_market(market)
                        if tid:
                            logger.info(f"Mercado encontrado vía CLOB Fallback: {name}")
                            return tid, name
        except Exception as e:
            logger.warning(f"Error en _fetch_clob: {e}")
        return None, ""

    async def get_active_btc_5min_market(self) -> Tuple[Optional[str], str]:
        logger.info("Iniciando escaneo de mercados (Capa 1: Gamma API)...")
        if self.dashboard:
            self.dashboard.add_event("[SCANNER] Intentando capa 1 (Gamma API)...")
        tid, name = await self._fetch_gamma()
        if tid:
            return tid, name
            
        logger.info("Iniciando escaneo de mercados (Capa 2: CLOB API Fallback)...")
        if self.dashboard:
            self.dashboard.add_event("[SCANNER] Intentando capa 2 (CLOB REST)...")
        tid, name = await self._fetch_clob()
        if tid:
            return tid, name
            
        logger.info("No se encontró ningún mercado activo de BTC a 5 minutos.")
        return None, ""
