import asyncio
import websockets
import json

# Delta Exchange India - public WebSocket feed (documented at docs.delta.exchange)
DELTA_WS_URL = "wss://socket.india.delta.exchange"


async def test_delta_ws():
    print("Connecting...")

    async with websockets.connect(DELTA_WS_URL) as ws:
        print("Connected.")

        # Testing both symbol formats at once so we don't have to guess -
        # whichever one actually returns data wins.
        sub_msg = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "mark_price", "symbols": ["MARK:BTCUSD", "BTCUSD"]},
                    {"name": "funding_rate", "symbols": ["BTCUSD"]},
                ]
            },
        }
        await ws.send(json.dumps(sub_msg))
        print(f"Sent: {json.dumps(sub_msg)}")

        print("\nListening for 20 seconds...\n")

        end_time = asyncio.get_event_loop().time() + 20
        count = 0

        while asyncio.get_event_loop().time() < end_time:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2)
                count += 1
                print(f"[{count}] {msg[:400]}")
            except asyncio.TimeoutError:
                remaining = int(end_time - asyncio.get_event_loop().time())
                print(f"...still listening, {remaining}s left")

        print(f"\nDone. Received {count} messages total.")


asyncio.run(test_delta_ws())
