import asyncio
import websockets
import json
import hashlib
import hmac
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("PI42_API_KEY")
API_SECRET = os.getenv("PI42_API_SECRET")

def generate_signature(secret, data):
    return hmac.new(
        secret.encode('utf-8'),
        data.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

async def test_pi42_ws():
    url = "wss://fawss.pi42.com/socket.io/?EIO=4&transport=websocket"
    
    print("Connecting...")
    
    async with websockets.connect(url) as ws:
        # Handshake
        await ws.recv()
        await ws.send("40")
        await ws.recv()
        print("Connected and handshake done")
        
        # Generate signature
        timestamp = str(int(time.time() * 1000))
        signature = generate_signature(API_SECRET, timestamp)
        
        # Send auth
        auth_msg = f'42["auth", {{"apiKey": "{API_KEY}", "timestamp": "{timestamp}", "signature": "{signature}"}}]'
        await ws.send(auth_msg)
        print(f"Sent auth: {auth_msg[:80]}...")
        
        # Immediately send multiple subscription attempts
        subs = [
            '42["subscribe", {"streams": ["markPriceArr"]}]',
            '42["subscribe", {"streams": ["btcinr@markPrice"]}]',
            '42["subscribe", {"streams": ["BTCINR@markPrice"]}]',
            '42["markPriceArr", {}]',
        ]
        
        for sub in subs:
            await ws.send(sub)
            print(f"Sent: {sub[:60]}")
        
        print("\nListening for ALL messages for 20 seconds...\n")
        
        end_time = asyncio.get_event_loop().time() + 20
        count = 0
        
        while asyncio.get_event_loop().time() < end_time:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2)
                count += 1
                print(f"[{count}] {msg[:600]}")
                print("---")
                
                if msg == "2":
                    await ws.send("3")
                    
            except asyncio.TimeoutError:
                remaining = int(end_time - asyncio.get_event_loop().time())
                print(f"...{remaining}s left")

asyncio.run(test_pi42_ws())