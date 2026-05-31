"""
execution_engine.py
Pre-firma ambas órdenes (YES/NO) durante la ventana idle y dispara la correcta
en los últimos N segundos del ciclo, minimizando latencia en el hot path.
"""

import asyncio
import time
import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY
from config import AppConfig

logger = logging.getLogger("polymarket_arb.execution")

# Ventana de ejecución y pre-firma
PRESIGN_AT_SECONDS_REMAINING = 60   # pre-firma cuando queda 1 minuto
FIRE_AT_SECONDS_REMAINING    = 8    # dispara cuando quedan 8 segundos
SLIPPAGE_TOLERANCE           = 0.03  # 3 centavos de tolerancia

@dataclass
class MarketContext:
    condition_id: str
    token_id_yes: str   # token ID del lado YES
    token_id_no: str    # token ID del lado NO
    open_ts: float
    open_price: float
    last_price: float

    @property
    def close_ts(self) -> float:
        return self.open_ts + 300.0

    def seconds_remaining(self) -> float:
        return max(0.0, self.close_ts - time.time())

    def expected_outcome(self) -> str:
        return "YES" if self.last_price >= self.open_price else "NO"

@dataclass
class PreSignedBundle:
    """Par de órdenes pre-firmadas, listas para HTTP POST."""
    signed_yes: dict
    signed_no: dict
    created_at: float
    presign_price_yes: float
    presign_price_no: float

class ExecutionEngine:
    def __init__(self, clob_client: ClobClient, config: AppConfig, order_size_usdc: float = 20.0):
        self.clob = clob_client
        self.config = config
        self.order_size_usdc = order_size_usdc
        self._bundle: Optional[PreSignedBundle] = None
        self._fired = False
        self._presigned = False
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=10, keepalive_timeout=30, enable_cleanup_closed=True)
            self._session = aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=3.0, connect=1.0))
        return self._session

    def _presign_orders(self, ctx: MarketContext) -> PreSignedBundle:
        t0 = time.perf_counter()

        current_price_yes = ctx.last_price / 100.0 if ctx.last_price > 1.0 else ctx.last_price
        current_price_no  = 1.0 - current_price_yes

        buy_price_yes = max(min(current_price_yes + SLIPPAGE_TOLERANCE, 0.99), 0.01)
        buy_price_no  = max(min(current_price_no  + SLIPPAGE_TOLERANCE, 0.99), 0.01)

        order_args_yes = OrderArgs(
            token_id=ctx.token_id_yes,
            price=buy_price_yes,
            size=self.order_size_usdc / buy_price_yes,
            side=BUY,
        )
        order_args_no = OrderArgs(
            token_id=ctx.token_id_no,
            price=buy_price_no,
            size=self.order_size_usdc / buy_price_no,
            side=BUY,
        )

        signed_yes = self.clob.create_order(order_args_yes)
        signed_no  = self.clob.create_order(order_args_no)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(f"[PRESIGN] Pre-firma completada en {elapsed_ms:.2f}ms")

        if hasattr(self, 'dashboard'):
            self.dashboard.add_event("🔒 [PRESIGN] T-60s | Órdenes EIP-712 pre-firmadas en memoria.")

        return PreSignedBundle(
            signed_yes=signed_yes,
            signed_no=signed_no,
            created_at=time.time(),
            presign_price_yes=current_price_yes,
            presign_price_no=current_price_no,
        )

    async def _fire_order(self, ctx: MarketContext):
        if self._fired:
            return

        outcome   = ctx.expected_outcome()
        signed    = self._bundle.signed_yes if outcome == "YES" else self._bundle.signed_no
        remaining = ctx.seconds_remaining()

        # Visual HUD for FIRE
        if self.config.execution.dry_run:
            hud_msg = f"🚀 [FIRE] T-{int(remaining)}s | SIMULACIÓN: Ejecutando {outcome} @ ${ctx.last_price:,.2f}"
            if hasattr(self, 'dashboard'):
                self.dashboard.add_event(hud_msg)
            logger.info(f"[FIRE SIMULATION] Disparando {outcome} @ last_price={ctx.last_price:.2f}")
        else:
            hud_msg = f"🚀 [FIRE] T-{int(remaining)}s | LIVE: Ejecutando {outcome} @ ${ctx.last_price:,.2f}"
            if hasattr(self, 'dashboard'):
                self.dashboard.add_event(hud_msg)
            logger.info(f"[FIRE LIVE] Disparando {outcome} @ last_price={ctx.last_price:.2f}")

        t0 = time.perf_counter()
        self._fired = True

        try:
            if self.config.execution.dry_run:
                # Simulamos latencia típica
                await asyncio.sleep(0.130)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                logger.info(f"[FILL] ✓ SIMULACIÓN Exitosa | HTTP round-trip={elapsed_ms:.1f}ms")
            else:
                resp = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.clob.post_order(signed, OrderType.FOK)
                )
                elapsed_ms = (time.perf_counter() - t0) * 1000

                if resp and resp.get("success"):
                    logger.info(f"[FILL] ✓ Order ID={resp.get('orderID')} | HTTP round-trip={elapsed_ms:.1f}ms")
                else:
                    logger.error(f"[FILL] ✗ Rechazada: {resp}")

        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.error(f"[FIRE] Error después de {elapsed_ms:.1f}ms: {e}")

    def reset(self):
        self._bundle    = None
        self._fired     = False
        self._presigned = False

    async def on_price_tick(self, ctx: MarketContext, price: float):
        ctx.last_price = price
        remaining = ctx.seconds_remaining()

        if ctx.last_price <= 0 or remaining <= 0:
            return

        if not self._presigned and remaining <= PRESIGN_AT_SECONDS_REMAINING:
            self._presigned = True
            loop = asyncio.get_event_loop()
            self._bundle = await loop.run_in_executor(None, self._presign_orders, ctx)

        elif (
            self._presigned
            and not self._fired
            and self._bundle is not None
            and remaining > FIRE_AT_SECONDS_REMAINING
        ):
            price_drift = abs(price - self._bundle.presign_price_yes * 100)
            if price_drift > 2.0:
                logger.info(f"[PRESIGN] Precio derivó {price_drift:.2f} → re-firmando")
                self._presigned = False

        if (
            not self._fired
            and self._bundle is not None
            and remaining <= FIRE_AT_SECONDS_REMAINING
        ):
            await self._fire_order(ctx)

    async def cleanup(self):
        if self._session and not self._session.closed:
            await self._session.close()
