"""
Test: does Pi42's websocket accept a USDT-denominated markPrice channel
(e.g. "btcusdt@markPrice") the same way it accepts the INR one
("btcinr@markPrice", already confirmed working throughout this project)?

Goal: if this works, the spread scanner (and potentially other parts of
this project) can pull Pi42 prices natively in USDT - avoiding the need
for any USD/INR currency conversion at all, which is more direct and
has one less moving part than converting through a live FX rate.

This is read-only and doesn't touch anything else. Standalone test,
same pattern used for every other new exchange integration in this
project - verify against real data before wiring it into anything live.
"""
import asyncio
import json
import time

import websockets

PI42_WS_URL = "wss://fawss.pi42.com/socket.io/?EIO=4&transport=websocket"

# A few reasonable guesses for the USDT channel name, tried together -
# whichever one(s) actually produce a markPriceUpdate response tells us
# the real convention, rather than assuming.
CANDIDATE_CHANNELS = [
    "btcusdt@markPrice",
    "BTCUSDT@markPrice",
    "btc_usdt@markPrice",
]


async def test_channels():
    results = {}
    async with websockets.connect(PI42_WS_URL) as ws:
        await ws.recv()
        await ws.send("40")
        await ws.recv()

        sub_msg = f'42["subscribe", {{"params": {json.dumps(CANDIDATE_CHANNELS)}}}]'
        print(f"Subscribing to: {CANDIDATE_CHANNELS}")
        await ws.send(sub_msg)

        end_time = asyncio.get_event_loop().time() + 20
        message_count = 0
        while asyncio.get_event_loop().time() < end_time:
            remaining = end_time - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            if msg == "2":
                await ws.send("3")
                continue
            if not msg.startswith("42["):
                continue
            message_count += 1
            payload = json.loads(msg[2:])
            event_name = payload[0]
            data = payload[1] if len(payload) > 1 else {}
            print(f"  [msg {message_count}] event={event_name}  raw={json.dumps(data)[:200]}")
            sym = data.get("s", "")
            if sym and sym not in results:
                results[sym] = data

    return results


if __name__ == "__main__":
    print("=" * 58)
    print("  PI42 USDT CHANNEL TEST")
    print("=" * 58)
    results = asyncio.run(test_channels())

    print("\n" + "=" * 58)
    if results:
        print(f"  SUCCESS - received data for symbol(s): {list(results.keys())}")
        for sym, data in results.items():
            print(f"\n  Symbol: {sym}")
            print(f"  Full payload: {json.dumps(data, indent=2)}")
    else:
        print("  NO DATA RECEIVED for any candidate channel in 20s.")
        print("  This means the USDT channel naming guess was wrong, or")
        print("  Pi42 doesn't expose USDT markPrice via this same socket.")
        print("  Next step would be checking Pi42's actual API docs directly")
        print("  rather than guessing further channel name variations.")
