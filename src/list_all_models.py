import os
from google import genai

API_KEY = os.getenv("GoogleAI-API-key")
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
client = genai.Client(
    vertexai=True,
    project=PROJECT_ID,
    location=LOCATION
)

print("Available Models:")
for model in client.models.list():
    print(f"- {model.name}")
