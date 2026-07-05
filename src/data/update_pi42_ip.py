import requests
import hmac
import hashlib
import time
import json
from dotenv import load_dotenv, set_key
import os

load_dotenv()

API_KEY = os.getenv("PI42_API_KEY")
API_SECRET = os.getenv("PI42_API_SECRET")
BASE_URL = "https://fapi.pi42.com"
ENV_FILE = os.path.join(os.path.dirname(__file__), "../../.env")

def generate_signature(secret, data):
    return hmac.new(
        secret.encode("utf-8"),
        data.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def post_request(endpoint, params):
    """Returns (status_code, parsed_json_or_raw_text)."""
    timestamp = str(int(time.time() * 1000))
    params["timestamp"] = timestamp
    body = json.dumps(params, separators=(",", ":"))
    signature = generate_signature(API_SECRET, body)
    url = f"{BASE_URL}{endpoint}"
    headers = {
        "api-key": API_KEY,
        "signature": signature,
        "Content-Type": "application/json"
    }
    response = requests.post(url, data=body, headers=headers, timeout=10)
    try:
        data = response.json()
    except Exception:
        data = {"raw_response": response.text}
    return response.status_code, data

def get_current_ip():
    response = requests.get("https://api.ipify.org?format=json", timeout=10)
    return response.json()["ip"]

def update_pi42_ip():
    print("Checking IP address...")

    current_ip = get_current_ip()
    last_known_ip = os.getenv("PI42_WHITELISTED_IP", "")

    print(f"Current IP  : {current_ip}")
    print(f"Last known  : {last_known_ip if last_known_ip else '(none saved yet)'}")

    if current_ip == last_known_ip:
        print("IP unchanged. No update needed.")
        return True

    # ── STEP 1: Add the new IP FIRST ─────────────────────────
    print(f"\nIP changed! Adding new IP: {current_ip}")
    status, add_result = post_request(
        "/v1/retail/add-allowed-ips",
        {"ip": current_ip, "apiKey": API_KEY}
    )
    print(f"Add result (HTTP {status}): {add_result}")

    add_ok = (status == 200) and ("error" not in add_result)

    if not add_ok:
        print("\n⚠ Add did NOT clearly succeed.")
        print("STOPPING here on purpose — your OLD IP is left untouched,")
        print("so you still have working API access. Nothing is broken.")
        print("Share this exact output so we can fix the request format.")
        return False

    print("✅ New IP added and confirmed.")

    # ── STEP 2: Only now remove the OLD IP ───────────────────
    if last_known_ip:
        print(f"\nRemoving old IP: {last_known_ip}")
        status2, remove_result = post_request(
            "/v1/retail/remove-allowed-ips",
            {"ip": last_known_ip, "apiKey": API_KEY}
        )
        print(f"Remove result (HTTP {status2}): {remove_result}")

    # ── STEP 3: Save the new IP as our reference point ───────
    env_path = os.path.abspath(ENV_FILE)
    set_key(env_path, "PI42_WHITELISTED_IP", current_ip)
    print(f"\nSaved {current_ip} to .env as the new reference IP.")

    return True

if __name__ == "__main__":
    update_pi42_ip()
