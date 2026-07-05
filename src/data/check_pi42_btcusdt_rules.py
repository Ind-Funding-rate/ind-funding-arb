import requests
import hmac
import hashlib
import time
from urllib.parse import urlencode
from dotenv import load_dotenv
import os
import json

load_dotenv()

API_KEY = os.getenv("PI42_API_KEY")
API_SECRET = os.getenv("PI42_API_SECRET")
BASE_URL = "https://fapi.pi42.com"

endpoint = "/v1/market/exchangeInfo"
timestamp = str(int(time.time() * 1000))

params = {"timestamp": timestamp}
query_string = urlencode(params)

signature = hmac.new(
    API_SECRET.encode("utf-8"),
    query_string.encode("utf-8"),
    hashlib.sha256
).hexdigest()

url = f"{BASE_URL}{endpoint}?{query_string}"
headers = {
    "api-key": API_KEY,
    "signature": signature,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Origin": "https://pi42.com",
    "Referer": "https://pi42.com/",
    "Accept": "application/json"
}

response = requests.get(url, headers=headers, timeout=10)
print(f"Status: {response.status_code}")

data = response.json()

# The response might be a list directly, or wrapped in a key - handle both
items = data if isinstance(data, list) else data.get("data", data.get("symbols", []))

found = False
for symbol_info in items:
    if isinstance(symbol_info, dict) and symbol_info.get("symbol") == "BTCUSDT":
        print(json.dumps(symbol_info, indent=2))
        found = True
        break

if not found:
    print("BTCUSDT not found. Showing first item structure for reference:")
    print(json.dumps(items[0] if items else data, indent=2)[:1500])
