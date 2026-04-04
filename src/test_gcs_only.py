import os
import sys
from google import genai
from google.genai import types

API_KEY = os.getenv("GoogleAI-API-key")
MODEL_NAME = "gemini-3-flash-preview"

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    sys.exit(1)

client = genai.Client(api_key=API_KEY)

def test_line():
    gcs_uri = "gs://fta-form26r-library/full-set/2010-2018/000044_BondiA_311_26R.pdf"
    print(f"Testing GCS URI ONLY (no cache) with {gcs_uri}...")
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=[
                types.Content(
                    role="user",
                    parts=[
                        types.Part.from_text(text="What is this?"),
                        types.Part.from_uri(file_uri=gcs_uri, mime_type="application/pdf")
                    ]
                )
            ]
        )
        print("Success (GCS Only)!")
        print(response.text[:200])
    except Exception as e:
        print(f"FAILED (GCS Only): {e}")

if __name__ == "__main__":
    test_line()
