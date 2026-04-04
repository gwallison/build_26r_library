import os
import sys
from google import genai
from google.genai import types

API_KEY = os.getenv("GoogleAI-API-key")
MODEL_NAME = "gemini-3-flash-preview"
CACHE_ID = "cachedContents/nvu80xthonydt25ycytyosmzhysfqlx9fexg52y8"

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    sys.exit(1)

client = genai.Client(api_key=API_KEY)

def test_line():
    print(f"Testing EMPTY GenerateContent with cache {CACHE_ID}...")
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            config=types.GenerateContentConfig(
                cached_content=CACHE_ID,
            ),
            contents=[]
        )
        print("Success (Empty)!")
        print(response.text[:200])
    except Exception as e:
        print(f"FAILED (Empty): {e}")

if __name__ == "__main__":
    test_line()
