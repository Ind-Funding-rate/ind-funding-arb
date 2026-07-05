from dotenv import load_dotenv
import os

load_dotenv()

keys = ["PI42_API_KEY", "PI42_API_SECRET", "DELTA_API_KEY", "DELTA_API_SECRET"]

for k in keys:
    v = os.getenv(k)
    if v and len(v) > 4:
        print(f"{k}: SET ({len(v)} chars)")
    else:
        print(f"{k}: ❌ EMPTY or NOT SET")
