"""
Delta Exchange WebSocket client.

Single responsibility: stay connected to Delta forever, and call on_update(...)
whenever a mark_price or funding_rate update arrives for a tracked pair.
Knows nothing about databases, files, or the rest of the app - just Delta's
documented public WebSocket feed.
"""
import asyncio
import json
import logging
import websockets

logger = logging.getLogger("delta_client")

DELTA_WS_URL = "wss://socket.india.delta.exchange"
RECONNECT_DELAY = 5
MAX_RECONNECT_DELAY = 60


async def listen(pairs, on_update):
    """
    pairs: list of dicts, each with at least "delta_symbol" and "delta_mark_symbol"
    on_update: function(exchange, symbol, mark_price=None, funding_rate=None)
               called whenever fresh data arrives. Runs forever, reconnecting
               automatically (with increasing delay) if the connection drops.
    """
    mark_symbols = [p["delta_mark_symbol"] for p in pairs]
    funding_symbols = [p["delta_symbol"] for p in pairs]
    mark_lookup = {p["delta_mark_symbol"]: p["delta_symbol"] for p in pairs}
    delay = RECONNECT_DELAY

    while True:
        try:
            async with websockets.connect(DELTA_WS_URL) as ws:
                sub_msg = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "mark_price", "symbols": mark_symbols},
                            {"name": "funding_rate", "symbols": funding_symbols},
                        ]
                    },
                }
                await ws.send(json.dumps(sub_msg))
                logger.info("connected")
                delay = RECONNECT_DELAY

                async for msg in ws:
                    data = json.loads(msg)
                    msg_type = data.get("type")

                    if msg_type == "mark_price":
                        real_symbol = mark_lookup.get(data.get("symbol"))
                        if real_symbol:
                            on_update("Delta", real_symbol, mark_price=float(data.get("price", 0)))

                    elif msg_type == "funding_rate" and data.get("symbol") in funding_symbols:
                        on_update("Delta", data["symbol"], funding_rate=float(data.get("funding_rate", 0)))

        except Exception as e:
            logger.warning(f"connection error: {e}. Reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY)
