import asyncio
import os
from collections import deque
from typing import Deque

class DashboardRenderer:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.events: Deque[str] = deque(maxlen=6)
        self._running = False
        self._task = None

    def add_event(self, event_msg: str):
        # Aseguramos que el evento encaje visualmente
        self.events.append(event_msg)

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self.render_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    async def render_loop(self):
        while self._running:
            self._render()
            await asyncio.sleep(0.5)

    def _render(self):
        os.system('clear' if os.name == 'posix' else 'cls')
        
        # Extraer estados
        dry_run = getattr(self.orchestrator.config.execution, 'dry_run', True) if hasattr(self.orchestrator, 'config') else True
        dry_run_text = "ON" if dry_run else "OFF"
        
        engine_state = "SHUTDOWN"
        pnl = 0.0
        wallet_balance = 0.0
        
        if self.orchestrator.arbitrage_engine:
            engine_state = self.orchestrator.arbitrage_engine.state.name
            
        if self.orchestrator.risk_manager:
            pnl = self.orchestrator.risk_manager._total_pnl_usd
            
        # Simulación de latencias (podrían leerse de metrics en el futuro)
        parse_ms = 0.12
        eval_ms = 0.35
        eip_ms = 2.10

        width = 80
        
        lines = []
        lines.append("╔" + "═" * (width - 2) + "╗")
        lines.append("║" + " 🔮 POLYMARKET HFT TACTICAL DASHBOARD ".center(width - 2) + "║")
        lines.append("╠" + "═" * (width - 2) + "╣")
        
        # Header Info
        header = f"  [DRY_RUN: {dry_run_text}] | Kill Switch: OFF | State: {engine_state}"
        lines.append("║" + header.ljust(width - 2) + "║")
        lines.append("╠" + "═" * (width - 2) + "╣")
        
        # Métricas principales
        metrics1 = f"  Wallet PnL : ${pnl:.2f} | Execution Mode: FOK (Maker)"
        metrics2 = f"  Asset      : BTC/USD 5m | Strategy: Front-Running EIP-712"
        lines.append("║" + metrics1.ljust(width - 2) + "║")
        lines.append("║" + metrics2.ljust(width - 2) + "║")
        lines.append("╠" + "═" * (width - 2) + "╣")
        
        # Telemetría
        telemetry = f"  [LATENCY] Parsing: {parse_ms}ms | Eval: {eval_ms}ms | EIP-712: {eip_ms}ms"
        lines.append("║" + telemetry.ljust(width - 2) + "║")
        lines.append("╠" + "═" * (width - 2) + "╣")
        
        # Registros de eventos
        lines.append("║" + " [RECENT EVENTS] ".center(width - 2) + "║")
        
        current_events = list(self.events)
        while len(current_events) < 6:
            current_events.append("")
            
        for ev in current_events:
            # Ljust to width-4 to account for "  " padding and borders
            lines.append("║  " + ev.ljust(width - 4) + "║")
            
        lines.append("╚" + "═" * (width - 2) + "╝")
        
        # Print atomicamente
        print("\n".join(lines))
