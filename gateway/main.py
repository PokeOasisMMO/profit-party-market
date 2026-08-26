from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .config import CONFIG
from .koda_memory import KodaMemory
from .market_state import MarketState
from .providers.alpaca_feed import AlpacaFeed
from .providers.treasury_yields import TreasuryYieldFeed
from .providers.cftc_positioning import CftcPositioningFeed
from .providers.topstep_market import TopstepMarketFeed


state = MarketState(CONFIG)
memory = KodaMemory(Path(__file__).resolve().parent / "koda_memory.db")
alpaca_feed = AlpacaFeed(CONFIG, state)
topstep_market = TopstepMarketFeed(CONFIG, state)
treasury_yields = TreasuryYieldFeed(CONFIG, state)
cftc_positioning = CftcPositioningFeed(CONFIG, state)
databento_feed = None
if CONFIG.databento_enabled:
    try:
        from .providers.databento_feed import DatabentoFeed

        databento_feed = DatabentoFeed(CONFIG, state)
    except ImportError:
        databento_feed = None
connections: set[WebSocket] = set()


async def broadcast_loop() -> None:
    interval = max(50, CONFIG.broadcast_interval_ms) / 1_000
    while True:
        snapshot = await state.snapshot()
        memory.observe(snapshot)
        snapshot["memory"] = memory.summary()
        payload = json.dumps(snapshot, separators=(",", ":"), allow_nan=False)
        disconnected: list[WebSocket] = []
        for websocket in tuple(connections):
            try:
                await websocket.send_text(payload)
            except Exception:
                disconnected.append(websocket)
        for websocket in disconnected:
            connections.discard(websocket)
        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    missing = CONFIG.missing_credentials()
    await state.set_gateway_error(
        f"Missing gateway credentials: {', '.join(missing)}. Add them to gateway/.env."
        if missing
        else None
    )
    if CONFIG.databento_enabled and databento_feed is None:
        error = "Databento is enabled but its optional package is not installed."
        await state.set_provider(
            "databento",
            connected=False,
            error=error,
        )
    starters = [
        topstep_market.start(),
        alpaca_feed.start(),
        treasury_yields.start(),
        cftc_positioning.start(),
    ]
    if databento_feed is not None:
        starters.append(databento_feed.start())
    await asyncio.gather(*starters)
    broadcaster = asyncio.create_task(broadcast_loop(), name="gateway-broadcast")
    try:
        yield
    finally:
        broadcaster.cancel()
        await asyncio.gather(broadcaster, return_exceptions=True)
        stoppers = [
            topstep_market.stop(),
            alpaca_feed.stop(),
            treasury_yields.stop(),
            cftc_positioning.stop(),
        ]
        if databento_feed is not None:
            stoppers.append(databento_feed.stop())
        await asyncio.gather(*stoppers, return_exceptions=True)
        memory.close()


app = FastAPI(
    title="Profit Party Real Data Gateway",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(CONFIG.allowed_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)
app.add_middleware(GZipMiddleware, minimum_size=1_024)


@app.get("/")
async def service_status() -> dict[str, object]:
    snapshot = await state.snapshot()
    return {
        "service": "Profit Party Market",
        "online": True,
        "mode": "public-read-only",
        "suggestionsOnly": True,
        "orderExecution": False,
        "status": snapshot["status"],
        "ready": snapshot["ready"],
        "message": snapshot["message"],
        "health": "/health",
        "websocket": "/ws",
    }


@app.get("/health")
async def health() -> dict[str, object]:
    snapshot = await state.snapshot()
    return {
        "service": "profit-party-real-data-gateway",
        "status": snapshot["status"],
        "ready": snapshot["ready"],
        "message": snapshot["message"],
        "providers": snapshot["providers"],
        "marketStreams": snapshot["marketStreams"],
        "instrument": snapshot["instrument"],
        "memory": memory.summary(),
        "macro": snapshot.get("macro"),
        "missingCredentials": CONFIG.missing_credentials(),
        "missingOptionalCredentials": CONFIG.missing_optional_credentials(),
    }


@app.websocket("/ws")
async def market_socket(websocket: WebSocket) -> None:
    origin = websocket.headers.get("origin")
    if origin and origin not in CONFIG.allowed_origins:
        await websocket.close(code=1008, reason="Origin is not allowed")
        return

    await websocket.accept()
    connections.add(websocket)
    try:
        await websocket.send_json(await state.snapshot())
        while True:
            message = await websocket.receive_text()
            if message == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        connections.discard(websocket)


def run() -> None:
    uvicorn.run(
        "gateway.main:app",
        host=CONFIG.host,
        port=CONFIG.port,
        reload=False,
        access_log=False,
        server_header=False,
    )


if __name__ == "__main__":
    run()
