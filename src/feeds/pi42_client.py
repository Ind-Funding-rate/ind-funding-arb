"""
Pi42 WebSocket client.

Single responsibility: stay connected to Pi42 forever, and call on_update(...)
whenever a markPriceUpdate arrives for a tracked pair. Knows nothing about
databases, files, or the rest of the app - just Pi42's wire format.
"""
import asyncio
import json
import logging
import websockets

logger = logging.getLogger("pi42_client")

PI42_WS_URL = "wss://fawss.pi42.com/socket.io/?EIO=4&transport=websocket"
RECONNECT_DELAY = 5
MAX_RECONNECT_DELAY = 60


async def listen(pairs, on_update):
    """
    pairs: list of dicts, each with at least "pi42_symbol" and "pi42_channel"
    on_update: function(exchange, symbol, mark_price=None, funding_rate=None)
               called whenever fresh data arrives. Runs forever, reconnecting
               automatically (with increasing delay) if the connection drops.
    """
    channels = [p["pi42_channel"] for p in pairs]
    tracked_symbols = {p["pi42_symbol"] for p in pairs}
    delay = RECONNECT_DELAY

    while True:
        try:
            async with websockets.connect(PI42_WS_URL) as ws:
                await ws.recv()        # Engine.IO handshake info
                await ws.send("40")    # confirm namespace connect
                await ws.recv()
                logger.info("connected")
                delay = RECONNECT_DELAY  # reset backoff after a successful connect

                sub_msg = f'42["subscribe", {{"params": {json.dumps(channels)}}}]'
                await ws.send(sub_msg)

                async for msg in ws:
                    if msg == "2":          # Engine.IO heartbeat ping
                        await ws.send("3")  # reply with pong
                        continue
                    if not msg.startswith("42["):
                        continue

                    payload = json.loads(msg[2:])
                    event_name = payload[0]
                    data = payload[1] if len(payload) > 1 else {}

                    if event_name == "markPriceUpdate" and data.get("s") in tracked_symbols:
                        on_update(
                            "Pi42", data["s"],
                            mark_price=float(data.get("p", 0)),
                            funding_rate=float(data.get("r", 0)),
                        )

        except Exception as e:
            logger.warning(f"connection error: {e}. Reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY)
