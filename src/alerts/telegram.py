import requests
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# ── SEND MESSAGE ─────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    """Send a message to Telegram. Returns True if successful."""
    if not BOT_TOKEN or not CHAT_ID:
        print("  [ALERT] Telegram not configured in .env")
        return False
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        response = requests.post(url, json={
            "chat_id":    CHAT_ID,
            "text":       message,
            "parse_mode": "HTML"
        }, timeout=5)
        return response.status_code == 200
    except Exception as e:
        print(f"  [ALERT ERROR] {e}")
        return False

# ── OPPORTUNITY ALERT (single pair, BTC-only executor) ────────
def send_opportunity_alert(gap_pct: float, net_pct: float,
                           breakeven_hrs: float,
                           delta_rate: float, pi42_rate: float):
    """Send alert when a profitable opportunity is detected."""
    message = (
        f"🚨 <b>FUNDING ARB OPPORTUNITY</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 <b>Delta Rate</b>  : {delta_rate*100:.4f}%\n"
        f"📉 <b>Pi42 Rate</b>   : {pi42_rate*100:.4f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>GAP</b>         : {gap_pct:.4f} pp\n"
        f"💰 <b>NET PROFIT</b>  : +{net_pct:.4f}%\n"
        f"⏱ <b>Breakeven</b>   : {breakeven_hrs:.1f} hrs\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    return send_telegram(message)

# ── MULTI-COIN OPPORTUNITY ALERT (full market scanner) ─────────
def send_multi_opportunity_alert(coin: str, gap_pct: float, net_pct: float,
                                  delta_rate: float, pi42_rate: float,
                                  delta_volume_usd: float):
    """Send alert when a profitable opportunity is detected on a specific
    coin during a full-market scan. Includes volume so a thin/illiquid
    false-positive can be judged at a glance."""
    message = (
        f"🚨 <b>FUNDING ARB OPPORTUNITY: {coin}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 <b>Delta Rate</b>  : {delta_rate*100:.4f}%\n"
        f"📉 <b>Pi42 Rate</b>   : {pi42_rate*100:.4f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>GAP</b>         : {gap_pct:.4f} pp\n"
        f"💰 <b>NET PROFIT</b>  : +{net_pct:.4f}%\n"
        f"💧 <b>Delta 24h Vol</b>: ${delta_volume_usd:,.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    return send_telegram(message)

# ── SYSTEM ALERT ──────────────────────────────────────────────
def send_system_alert(message: str):
    """Send a system status alert."""
    return send_telegram(f"⚙️ <b>SYSTEM</b>: {message}")

# ── TEST ──────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Testing Telegram alert...")
    success = send_opportunity_alert(
        gap_pct=0.992014,
        net_pct=0.6852,
        breakeven_hrs=2.5,
        delta_rate=0.01,
        pi42_rate=0.00008,
    )
    if success:
        print("✅ Alert sent! Check your Telegram.")
    else:
        print("❌ Failed. Check your .env file.")
