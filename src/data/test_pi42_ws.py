import asyncio
import websockets
import json

PI42_WS_URL = "wss://fawss.pi42.com/socket.io/?EIO=4&transport=websocket"


async def test_pi42_ws():
    print("Connecting...")

    async with websockets.connect(PI42_WS_URL) as ws:
        # Engine.IO handshake (this part always worked)
        await ws.recv()        # server sends "0{...}" handshake info
        await ws.send("40")    # client confirms namespace connect
        await ws.recv()        # server confirms "40{...}"
        print("Connected and handshake done")

        # THE FIX: real site sends key "params", not "streams"
        # (captured directly from browser DevTools on pi42.com/futures/btcinr)
        sub_msg = '42["subscribe", {"params": ["btcinr@markPrice"]}]'
        await ws.send(sub_msg)
        print(f"Sent: {sub_msg}")

        print("\nListening for BTCINR markPriceUpdate for 20 seconds...\n")

        end_time = asyncio.get_event_loop().time() + 20
        count = 0

        while asyncio.get_event_loop().time() < end_time:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2)

                # Engine.IO heartbeat frames - just reply, nothing to show
                if msg in ("2", "3"):
                    if msg == "2":
                        await ws.send("3")
                    continue

                # Real events look like: 42["eventName", {...}]
                if msg.startswith("42["):
                    payload = json.loads(msg[2:])
                    event_name = payload[0]
                    data = payload[1] if len(payload) > 1 else {}

                    if event_name == "markPriceUpdate" and data.get("s") == "BTCINR":
                        count += 1
                        print(
                            f"[{count}] funding_rate={data.get('r')}  "
                            f"mark_price={data.get('p')}  "
                            f"next_funding_time={data.get('T')}"
                        )

            except asyncio.TimeoutError:
                remaining = int(end_time - asyncio.get_event_loop().time())
                print(f"...still listening, {remaining}s left")

        print(f"\nDone. Received {count} BTCINR markPriceUpdate messages.")


asyncio.run(test_pi42_ws())
