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
    # Use a real file from the user's previous successful web_fetch or similar
    gcs_uri = "gs://fta-form26r-library/full-set/2010-2018/000044_BondiA_311_26R.pdf"
    prompt = "Extract the data from this form." # Simplified for test

    print(f"Testing GenerateContent with cache {CACHE_ID}...")
    
    try:
        # Testing the EXACT structure we put in JSONL
        # contents must be a list of Content objects
        response = client.models.generate_content(
            model=MODEL_NAME,
            config=types.GenerateContentConfig(
                cached_content=CACHE_ID,
            ),
            contents=[
                types.Content(
                    role="user",
                    parts=[
                        types.Part.from_text(text=prompt),
                        types.Part.from_uri(file_uri=gcs_uri, mime_type="application/pdf")
                    ]
                )
            ]
        )
        print("Success!")
        print(response.text[:200])
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    test_line()
