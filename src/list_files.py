import os
from google import genai

API_KEY = os.getenv("GoogleAI-API-key")
client = genai.Client(api_key=API_KEY)

for file in client.files.list():
    print(f"File: {file.display_name}, Name: {file.name}, URI: {file.uri}")
    break
