import os
from google import genai

API_KEY = os.getenv("GoogleAI-API-key")
client = genai.Client(api_key=API_KEY)

for model in client.models.list():
    if any('cache' in action.lower() for action in model.supported_actions):
        print(f"Model: {model.name}")
        print(f"Actions: {model.supported_actions}")
