import os
from google import genai

API_KEY = os.getenv("GoogleAI-API-key")
client = genai.Client(api_key=API_KEY)

print("Listing models that support context caching:")
for model in client.models.list():
    if 'createCachedContent' in model.supported_generation_methods:
        print(f"- {model.name}")
