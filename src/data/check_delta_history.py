import sqlite3

conn = sqlite3.connect("data/rates.db")
rows = conn.execute(
    "SELECT timestamp, mark_price, funding_rate FROM funding_rates "
    "WHERE exchange='Delta' ORDER BY id"
).fetchall()

print(f"Total Delta records: {len(rows)}")

vals = [r[2] for r in rows if r[2] is not None]
distinct = sorted(set(vals))
print(f"Distinct funding_rate values ever seen: {distinct}")

if rows:
    print(f"First record: {rows[0]}")
    print(f"Last record:  {rows[-1]}")

# Show the most recent 10 to see the trend over time
print("\nLast 10 records:")
for r in rows[-10:]:
    print(r)
