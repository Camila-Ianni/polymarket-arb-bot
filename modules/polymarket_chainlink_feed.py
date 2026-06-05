import asyncio
import json
import logging
import time
import websockets
from typing import Callable, Optional

logger = logging.getLogger("polymarket_arb.chainlink_feed")

WS_RTDS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market" # Placeholder URL or known standard for Polymarket RTDS

class MarketTimer:
    def __init__(self, start_time: float):
        self.start_time = start_time
        
    def get_time_remaining(self, duration_s: float = 300.0) -> float:
        elapsed = time.time() - self.start_time
        return max(0.0, duration_s - elapsed)

class ChainlinkRTDSFeed:
    def __init__(self):
        self.market_timer: Optional[MarketTimer] = None
        self.on_signal: Optional[Callable] = self.default_signal_handler
        self._running = False
        self._task: Optional[asyncio.Task] = None
        
        # Estado mock para simular precios y variaciones
        self.last_price = 67200.00
    
    def set_market_timer(self, timer: MarketTimer):
        self.market_timer = timer

    def default_signal_handler(self, price: float, delta_pct: float, direction: str):
        time_rem = self.market_timer.get_time_remaining() if self.market_timer else 0.0
        # Format explicitly as requested: ║ ⚡ [SIGNAL] T-8.5s | BTC: $67,234.50 | Δ: +0.12% → UP ║
        sign = "+" if delta_pct >= 0 else ""
        logger.info(f"⚡ [SIGNAL] T-{time_rem:.1f}s | BTC: ${price:,.2f} | Δ: {sign}{delta_pct:.2f}% → {direction}")

    async def start(self):
        self._running = True
        logger.info("Iniciando Binance Live Feed (BTCUSDT)...")
        self._task = asyncio.create_task(self._binance_feed_loop())
        
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("Binance Live Feed detenido.")

    async def _binance_feed_loop(self):
        """
        Conecta al WebSocket de Binance para obtener precios reales de BTC en vivo.
        Actualiza el estado interno para el Dashboard y emite señales si hay un mercado activo.
        """
        ws_url = "wss://stream.binance.com:9443/ws/btcusdt@miniTicker"
        
        while self._running:
            try:
                async with websockets.connect(ws_url, ping_interval=10, ping_timeout=5) as ws:
                    logger.debug("Conectado a Binance WebSocket (BTCUSDT)")
                    while self._running:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        
                        if "c" in data:
                            new_price = float(data["c"])
                            
                            if self.last_price > 0:
                                variation = ((new_price - self.last_price) / self.last_price) * 100
                            else:
                                variation = 0.0
                                
                            self.last_price = new_price
                            direction = "UP" if variation >= 0 else "DOWN"
                            
                            # Solo emitimos la señal y procesamos lógica si hay un mercado enganchado
                            if self.market_timer and self.on_signal:
                                if asyncio.iscoroutinefunction(self.on_signal):
                                    await self.on_signal(self.last_price, variation, direction)
                                else:
                                    self.on_signal(self.last_price, variation, direction)
                                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Desconexión de Binance WS: {e}. Reconectando en 3s...")
                await asyncio.sleep(3.0)
