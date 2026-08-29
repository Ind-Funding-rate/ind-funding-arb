"""
Delta Exchange WebSocket client.

Single responsibility: stay connected to Delta forever, and call on_update(...)
whenever a mark_price or funding_rate update arrives for a tracked pair.
Knows nothing about databases, files, or the rest of the app - just Delta's
documented public WebSocket feed.

2026-08-29 - added plain print() diagnostics (connect confirmation +
first few raw messages) alongside the existing logger calls. The
logger.info()/logger.warning() calls alone are silent by default (no
handler configured at that level), which made a real "is this even
connecting" question impossible to answer from server console output
alone during Step 1 testing. These prints are intentionally bounded
(only the first 5 raw messages) so they don't spam the live console
forever.
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

    print(f"[delta_client] starting - tracking {len(pairs)} symbols "
          f"(e.g. {funding_symbols[:3]})")

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
                print(f"[delta_client] connected to {DELTA_WS_URL} and sent subscribe "
                      f"request ({len(mark_symbols)} mark_price + {len(funding_symbols)} "
                      f"funding_rate symbols)")
                logger.info("connected")
                delay = RECONNECT_DELAY

                msg_count = 0
                async for msg in ws:
                    data = json.loads(msg)
                    msg_count += 1
                    if msg_count <= 5:
                        print(f"[delta_client] raw message #{msg_count}: {data}")
                    msg_type = data.get("type")

                    if msg_type == "mark_price":
                        real_symbol = mark_lookup.get(data.get("symbol"))
                        if real_symbol:
                            on_update("Delta", real_symbol, mark_price=float(data.get("price", 0)))

                    elif msg_type == "funding_rate" and data.get("symbol") in funding_symbols:
                        on_update("Delta", data["symbol"], funding_rate=float(data.get("funding_rate", 0)))

        except Exception as e:
            print(f"[delta_client] connection error: {e}. Reconnecting in {delay}s...")
            logger.warning(f"connection error: {e}. Reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY)
