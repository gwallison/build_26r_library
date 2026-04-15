import os
from google import genai

API_KEY = os.getenv("GoogleAI-API-key")
client = genai.Client(api_key=API_KEY)

for model in client.models.list():
    print(f"Model: {model.name}")
    # Inspect the object
    print(dir(model))
    break # Just one to check
