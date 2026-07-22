import requests
r = requests.get("https://api.india.delta.exchange/v2/tickers/BTCUSD", timeout=10)
print("STATUS:", r.status_code)
print(r.text)
