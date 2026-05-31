"""
main.py - Orquestador principal del bot de arbitraje de Polymarket.

Este módulo inicializa todos los componentes, gestiona el event loop de asyncio,
y asegura un shutdown graceful cuando se recibe una señal de interrupción.

ARQUITECTURA DE INICIALIZACIÓN:
1. Cargar configuración desde .env
2. Configurar logging profesional
3. Inicializar componentes (Feed, Monitor, Engine, Executor, Risk)
4. Conectar callbacks entre componentes
5. Iniciar event loop
6. Esperar señales de shutdown (SIGINT/SIGTERM)
7. Cleanup graceful de todos los recursos

HOT PATH CONSIDERATIONS:
- Este archivo NO está en el hot path
- Su única responsabilidad es orquestación lifecycle
- Todo el procesamiento real está en los módulos especializados
"""

import asyncio
import signal
import sys
import time
from typing import Optional
import logging

from config import get_config, AppConfig
from logging_config import setup_logging, get_logger, get_latency_logger
from models import OrderBookSnapshot

from modules.arbitrage_engine import ArbitrageEngine, EngineState
from modules.risk_manager import RiskManager
from modules.polymarket_chainlink_feed import ChainlinkRTDSFeed, MarketTimer
from modules.execution_engine import ExecutionEngine, MarketContext

# =============================================================================
# CONFIGURACIÓN INICIAL
# =============================================================================

logger = get_logger(__name__)
latency_logger = get_latency_logger(__name__)


class BotOrchestrator:
    """
    Orquestador principal del bot.

    RESPONSABILIDADES:
    1. Inicializar todos los componentes
    2. Gestionar el lifecycle (start/stop)
    3. Manejar señales de interrupción (Ctrl+C, SIGTERM)
    4. Logging periódico de estado y métricas
    5. Health checking de componentes
    """

    def __init__(self, config: Optional[AppConfig] = None, order_size_usdc: float = 20.0):
        """
        Inicializa el orquestador.

        Args:
            config: Configuración (usa global si None)
            order_size_usdc: Tamaño de cada orden (USDC)
        """
        self.config = config or get_config()
        self.order_size_usdc = order_size_usdc

        # Componentes (se inicializan en _initialize_components)
        self.weather_feed: Optional[FastWeatherFeed] = None
        self.polymarket_monitor: Optional[PolymarketMonitor] = None
        self.risk_manager: Optional[RiskManager] = None
        self.arbitrage_engine: Optional[ArbitrageEngine] = None
        self.chainlink_feed: Optional[ChainlinkRTDSFeed] = None
        self.execution_engine: Optional[ExecutionEngine] = None

        # Estado del orquestador
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Tareas de background
        self._health_check_task: Optional[asyncio.Task] = None
        self._metrics_log_task: Optional[asyncio.Task] = None

        # Señales de shutdown
        self._shutdown_signals = [signal.SIGINT, signal.SIGTERM]

        logger.info("BotOrchestrator inicializado")

    async def _initialize_components(self) -> None:
        """
        Inicializa todos los componentes del sistema.

        El orden es importante:
        1. Risk Manager (no depende de nadie)
        2. Web3 Executor (depende de config)
        3. Arbitrage Engine (depende de Risk + Executor)
        4. Weather Feed (independiente)
        5. Polymarket Monitor (independiente)
        """
        logger.info("Inicializando componentes...")
        
        # 0. Dashboard Renderer primero para feedback inmediato
        from modules.dashboard import DashboardRenderer
        self.dashboard = DashboardRenderer(self)
        self.dashboard.add_event("🟡 INICIALIZANDO Subsistemas...")
        asyncio.create_task(self.dashboard.start())

        with latency_logger.measure("component_initialization"):
            # 1. Risk Manager
            self.risk_manager = RiskManager(
                config=self.config,
                dry_run=self.config.execution.dry_run,
            )
            logger.debug("Risk Manager inicializado")

            # 3. Arbitrage Engine (Maker Mode con CLOB API)
            self.arbitrage_engine = ArbitrageEngine(
                config=self.config,
                risk_manager=self.risk_manager,
            )
            logger.debug("Arbitrage Engine inicializado (Maker Mode)")

            # 4. Chainlink RTDS Feed
            self.chainlink_feed = ChainlinkRTDSFeed()
            logger.debug("Chainlink RTDS Feed inicializado")

            # 5. Execution Engine (EIP-712 pre-signing)
            self.execution_engine = ExecutionEngine(
                clob_client=self.arbitrage_engine.clob_client,
                config=self.config,
                order_size_usdc=self.order_size_usdc
            )
            
            # Conectar Feed con ExecutionEngine
            async def _on_price_tick(price: float, delta: float, direction: str):
                if self.chainlink_feed and self.chainlink_feed.market_timer:
                    ctx = MarketContext(
                        condition_id="simulated",
                        token_id_yes="12345",
                        token_id_no="67890",
                        open_ts=self.chainlink_feed.market_timer.start_time,
                        open_price=self.chainlink_feed.last_price,
                        last_price=price,
                    )
                    await self.execution_engine.on_price_tick(ctx, price)
                    
                    if hasattr(self, 'dashboard'):
                        sign = "+" if delta >= 0 else ""
                        self.dashboard.add_event(f"⚡ [SIGNAL] BTC: ${price:,.2f} | Δ: {sign}{delta:.2f}% → {direction}")

            self.chainlink_feed.on_signal = _on_price_tick
            logger.debug("Execution Engine inicializado y conectado")

            self.execution_engine.dashboard = self.dashboard
            # 7. Market Scanner
            from modules.market_scanner import MarketScanner
            self.scanner = MarketScanner(dashboard=self.dashboard)

            logger.info("Componentes inicializados")



    async def _on_market_update(self, snapshot: OrderBookSnapshot) -> None:
        """
        Callback cuando hay actualización del order book.

        Reenvía los datos al ArbitrageEngine para procesamiento.
        """
        if self.arbitrage_engine and getattr(self.arbitrage_engine, 'state', None) == getattr(EngineState, 'RUNNING', None):
            await self.arbitrage_engine.submit_market_data(snapshot)

    async def _on_arbitrage_signal(self, signal) -> None:
        """
        Callback cuando se detecta una oportunidad de arbitraje.

        Útil para logging externo, métricas, o integración con sistemas de monitoreo.
        """
        logger.info(
            f"📊 Señal detectada: {signal.signal_type.name} | "
            f"ROI={signal.expected_roi:.2%} | "
            f"net_profit=${signal.net_expected_profit:.2f} | "
            f"urgency={signal.urgency_score:.2f}"
        )

    async def _health_check_loop(self) -> None:
        """
        Loop de health checking de componentes.

        Verifica que todos los componentes estén vivos y reporta estado.
        """
        while self._running:
            try:
                await asyncio.sleep(5)  # Chequear cada 5 segundos

                # Verificar componentes
                issues = []

                if self.weather_feed:
                    if self.weather_feed.state == WeatherFeedState.HEARTBEAT_TIMEOUT:
                        issues.append("Weather Feed: heartbeat timeout")
                    elif self.weather_feed.state == WeatherFeedState.ERROR:
                        issues.append("Weather Feed: error")

                if self.polymarket_monitor:
                    if self.polymarket_monitor.state == PolymarketMonitorState.ERROR:
                        issues.append("Polymarket Monitor: error")
                    elif self.polymarket_monitor.state == PolymarketMonitorState.RECONNECTING:
                        issues.append("Polymarket Monitor: reconectando")

                if self.arbitrage_engine:
                    engine_state = getattr(self.arbitrage_engine, 'state', None)
                    if getattr(engine_state, 'name', '') == 'ERROR' or engine_state == getattr(EngineState, 'ERROR', None):
                        issues.append("Arbitrage Engine: error")
                    elif getattr(engine_state, 'name', '') == 'PAUSED' or engine_state == getattr(EngineState, 'PAUSED', None):
                        issues.append("Arbitrage Engine: pausado (circuit breaker)")

                if issues:
                    logger.warning(f"⚠️ Health check issues: {', '.join(issues)}")
                else:
                    logger.debug("✓ Health check OK")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en health check: {e}", exc_info=True)

    async def _trading_cycle_loop(self) -> None:
        """
        Ciclo principal de evaluación y simulación (Strict 5 minutes).
        Extrae datos EXCLUSIVAMENTE del CLOB SDK oficial y muestra el Dashboard.
        """
        import traceback
        from datetime import datetime

        logger.info("Iniciando ciclo de evaluación de 5 minutos sobre mercados CLOB...")

        while self._running:
            try:
                if self.arbitrage_engine and self.arbitrage_engine.clob_client:
                    client = self.arbitrage_engine.clob_client
                    
                    ts = datetime.now().strftime('%H:%M:%S')
                    
                    if hasattr(self, 'dashboard'):
                        self.dashboard.add_event(f"[{ts}] Esperando próximo mercado 5m...")
                        
                    token_id, market_name = await self.scanner.get_active_btc_5min_market()
                    
                    if token_id:
                        if isinstance(token_id, dict) and token_id.get("condition_id") == "0xSIMULATED_CONDITION_ID":
                            if hasattr(self, 'dashboard'):
                                self.dashboard.add_event(f"[{ts}] [SIMULADOR] Reloj interno corriendo (90s)...")
                            
                            # Set open_ts to time.time() - 210 so close_ts (open_ts + 300) is in 90 seconds
                            ctx = MarketContext(
                                condition_id=token_id["condition_id"],
                                token_id_yes=token_id["token_id_yes"],
                                token_id_no=token_id["token_id_no"],
                                open_ts=time.time() - 210.0,
                                open_price=67000.0,
                                last_price=67000.0
                            )
                            
                            while time.time() < ctx.close_ts:
                                if self.execution_engine:
                                    await self.execution_engine.on_price_tick(ctx, 67000.0)
                                await asyncio.sleep(1)
                                
                            continue

                        if hasattr(self, 'dashboard'):
                            self.dashboard.add_event(f"[{ts}] Mercado enganchado: {market_name[:30]}...")
                            
                        if self.chainlink_feed and not self.chainlink_feed.market_timer:
                            self.chainlink_feed.set_market_timer(MarketTimer(start_time=time.time()))
                            
                        try:
                            # 1. Oráculo Nativo: Order Book del CLOB
                            ob = await client.get_order_book(token_id)
                            
                            best_bid = float(ob.bids[0].price) if hasattr(ob, 'bids') and ob.bids else 0.0
                            best_ask = float(ob.asks[0].price) if hasattr(ob, 'asks') and ob.asks else 0.0
                            
                            if best_bid and best_ask:
                                mid_price = (best_bid + best_ask) / 2
                                spread = best_ask - best_bid
                                
                                # Simulación de orden Maker
                                order_status = "Maker POST_ONLY Emitida" if spread > 0.01 else "Spread Insuficiente"
                                
                                # Simulamos la colocación si spread es bueno
                                if spread > 0.01:
                                    await client.place_order(token_id, "BUY", best_bid + 0.001, 5.0)
                            else:
                                mid_price = 0.0
                                spread = 0.0
                                order_status = "Sin Liquidez (Ignorado)"
                                
                            short_name = "BTC 5m"
                            if hasattr(self, 'dashboard'):
                                self.dashboard.add_event(f"[{ts}] {short_name} | Mid: {mid_price:.4f} | Status: {order_status}")
                            
                        except Exception as e:
                            # Manejo Estricto de Errores
                            logger.error(f"Error en CLOB eval: {e}", exc_info=True)
                            if hasattr(self, 'dashboard'):
                                self.dashboard.add_event(f"[{ts}] Error evaluando CLOB: {str(e)[:40]}")

                await asyncio.sleep(300)  # Strict 5 minutes

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[SYSTEM ERROR] Fallo crítico en el loop de trading: {e}", exc_info=True)
                await asyncio.sleep(30)

    def _setup_signal_handlers(self) -> None:
        """
        Configura handlers para señales de shutdown.

        Permite shutdown graceful con Ctrl+C o SIGTERM.
        """
        loop = asyncio.get_event_loop()

        for sig in self._shutdown_signals:
            loop.add_signal_handler(sig, self._handle_shutdown_signal)

        logger.debug(f"Signal handlers configurados para: {self._shutdown_signals}")

    def _handle_shutdown_signal(self) -> None:
        """
        Maneja una señal de shutdown.

        Sets the shutdown event para iniciar cleanup graceful.
        """
        logger.info("Señal de shutdown recibida")
        self._shutdown_event.set()

    async def start(self) -> None:
        """
        Inicia todos los componentes del bot.

        Se ejecuta hasta que se recibe una señal de shutdown.
        """
        logger.info("=" * 60)
        logger.info("🚀 INICIANDO POLYMARKET ARBITRAGE BOT")
        logger.info("=" * 60)

        start_time = time.time()

        # Inicializar componentes
        await self._initialize_components()

        # Configurar signal handlers
        self._setup_signal_handlers()

        # Iniciar componentes
        logger.info("Iniciando componentes...")

        # 3. Iniciar Arbitrage Engine
        await self.arbitrage_engine.start()

        # 4. Iniciar Chainlink Feed
        await self.chainlink_feed.start()
        
        if hasattr(self, 'dashboard'):
            await self.dashboard.start()

        # Iniciar tareas de background
        self._running = True
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._metrics_log_task = asyncio.create_task(self._trading_cycle_loop())

        init_time = time.time() - start_time
        logger.info(f"✅ Bot iniciado en {init_time:.2f}s")
        logger.info("=" * 60)

        # Loguear configuración clave
        logger.info(f"Modo: {'DRY RUN (simulación)' if self.config.execution.dry_run else 'LIVE (capital real)'}")
        logger.info(f"Condition ID: {self.config.polymarket.condition_id}")
        logger.info(f"Market IDs: {self.config.polymarket.market_ids}")
        logger.info(f"Bet size: ${self.config.trading.bet_size_usd}")
        logger.info(f"Min ROI: {self.config.trading.min_roi_threshold:.2%}")
        logger.info(f"Max slippage: {self.config.trading.max_slippage_tolerance:.2%}")
        logger.info("=" * 60)

        # Esperar señal de shutdown
        logger.info("Bot corriendo. Presiona Ctrl+C para detener...")
        await self._shutdown_event.wait()

        # Iniciar shutdown
        await self.stop()

    async def stop(self) -> None:
        """
        Detiene todos los componentes gracefulmente.

        Asegura cleanup de recursos y logging final.
        """
        logger.info("=" * 60)
        logger.info("🛑 DETENIENDO BOT...")
        logger.info("=" * 60)

        self._running = False

        if hasattr(self, 'dashboard'):
            await self.dashboard.stop()

        # Detener tareas de background
        for task in [self._health_check_task, self._metrics_log_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Detener componentes en orden inverso a inicialización
        logger.info("Deteniendo componentes...")

        # Detener Feed primero para evitar nuevas señales
        if self.chainlink_feed:
            await self.chainlink_feed.stop()

        # 1. Detener Engine (primero para dejar de generar señales)
        if self.arbitrage_engine:
            await self.arbitrage_engine.stop()

        # Loguear métricas finales crudas al log (archivo)
        if self.arbitrage_engine:
            logger.info(f"Engine Final Summary: {self.arbitrage_engine.get_engine_summary()}")
        if self.risk_manager:
            logger.info(f"Risk Final Summary: {self.risk_manager.get_risk_summary()}")

        # UI HUD Console
        width = 58
        print("\n" + "╔" + "═" * width + "╗")
        print("║" + " 📊 MÉTRICAS FINALES ".center(width) + "║")
        print("╠" + "═" * width + "╣")

        if self.arbitrage_engine:
            summary = self.arbitrage_engine.get_engine_summary()
            print("║" + "  [ENGINE]".ljust(width) + "║")
            for k, v in summary.items():
                if isinstance(v, dict): continue
                line = f"    {k.replace('_', ' ').title():<25}: {v}"
                print("║" + line.ljust(width) + "║")
            print("║" + " " * width + "║")

        if self.risk_manager:
            risk_summary = self.risk_manager.get_risk_summary()
            print("║" + "  [RISK]".ljust(width) + "║")
            for k, v in risk_summary.items():
                if isinstance(v, dict): continue
                line = f"    {k.replace('_', ' ').title():<25}: {v}"
                print("║" + line.ljust(width) + "║")

        print("╚" + "═" * width + "╝")
        print("✅ Apagado del sistema completado.")
        logger.info("Bot detenido correctamente")


# =============================================================================
# PUNTO DE ENTRADA PRINCIPAL
# =============================================================================

async def main(order_size_usdc: float) -> None:
    """
    Función main asíncrona.

    Configura logging, carga configuración, y ejecuta el orquestador.
    """
    # Cargar configuración
    config = get_config()

    # Configurar logging
    setup_logging(
        log_level=config.execution.log_level,
        log_file_path=config.execution.log_file_path,
        enable_json=False,  # True para producción con ELK/Datadog
    )

    logger.info("Iniciando aplicación...")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Config loaded: dry_run={config.execution.dry_run}")

    # Crear y ejecutar orquestador
    orchestrator = BotOrchestrator(config, order_size_usdc=order_size_usdc)

    try:
        await orchestrator.start()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt recibido")
    except Exception as e:
        logger.critical(f"Error crítico: {e}", exc_info=True)
        raise
    finally:
        # Cleanup final
        logger.info("Cleanup final...")


def run() -> None:
    """
    Función de entrada para ejecutar desde CLI.
    """
    try:
        # Bloqueo Pre-Inicio
        while True:
            try:
                user_input = input("💰 Ingresa el tamaño de la apuesta por trade (USDC): ")
                order_size_usdc = float(user_input)
                if order_size_usdc <= 0:
                    print("El tamaño de la apuesta debe ser mayor a 0.")
                    continue
                break
            except ValueError:
                print("Por favor ingresa un número válido.")
                
        print("\n⏳ Autenticando con Polymarket y levantando subsistemas, por favor espera...")
        
        asyncio.run(main(order_size_usdc))
    except KeyboardInterrupt:
        print("\nBot detenido por el usuario")
        sys.exit(0)
    except Exception as e:
        print(f"Error crítico: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
