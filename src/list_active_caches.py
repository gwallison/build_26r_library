import os
from google import genai

API_KEY = os.getenv("GoogleAI-API-key")
client = genai.Client(api_key=API_KEY)

print("Listing active context caches:")
try:
    # Use client.caches.list() to get active caches
    for cache in client.caches.list():
        print(f"Name: {cache.name}")
        print(f"  Model: {cache.model}")
        print(f"  Display Name: {cache.display_name}")
        print(f"  Expires: {cache.expire_time}")
        print("-" * 20)
except Exception as e:
    print(f"Error listing caches: {e}")
