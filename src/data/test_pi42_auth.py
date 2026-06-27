import os
from dotenv import load_dotenv

# Load keys from .env file
load_dotenv()

api_key = os.getenv("PI42_API_KEY")
api_secret = os.getenv("PI42_API_SECRET")

# Check they loaded correctly (never print the full secret)
if api_key and api_secret:
    print(f"✅ API Key loaded: {api_key[:6]}...")
    print(f"✅ API Secret loaded: {api_secret[:6]}...")
    print("Keys are ready to use.")
else:
    print("❌ Keys not found. Check your .env file.")