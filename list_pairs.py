import requests

print("=" * 54)
print("  DELTA - perpetual futures products")
print("=" * 54)
r = requests.get("https://api.india.delta.exchange/v2/products?contract_types=perpetual_futures", timeout=15).json()
delta_symbols = []
for p in r.get("result", []):
    sym = p.get("symbol", "")
    delta_symbols.append(sym)
print(f"Total: {len(delta_symbols)}")
print(sorted(delta_symbols))

print()
print("=" * 54)
print("  PI42 - available contracts")
print("=" * 54)
r2 = requests.get("https://api.pi42.com/v1/exchange/exchangeInfo?market=INR", timeout=15).json()
pi42_symbols = []
for c in r2.get("contracts", []):
    name = c.get("name", "")
    pi42_symbols.append(name)
print(f"Total: {len(pi42_symbols)}")
print(sorted(pi42_symbols))

print()
print("=" * 54)
print("  OVERLAP (same base coin on both exchanges)")
print("=" * 54)
delta_bases = set()
for s in delta_symbols:
    if s.endswith("USD"):
        delta_bases.add(s[:-3])

pi42_bases = set()
for s in pi42_symbols:
    if s.endswith("INR"):
        pi42_bases.add(s[:-3])

overlap = sorted(delta_bases & pi42_bases)
print(f"Coins on BOTH Delta (xUSD) and Pi42 (xINR): {len(overlap)}")
print(overlap)
