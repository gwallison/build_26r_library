import os
import sys
from google import genai
from google.genai import types

API_KEY = os.getenv("GoogleAI-API-key")
MODEL_NAME = "gemini-3-flash-preview"
PDF_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    sys.exit(1)

client = genai.Client(api_key=API_KEY)

def test_line():
    file_path = os.path.join(PDF_ROOT, "2010-2018", "000044_BondiA_311_26R.pdf")
    print(f"Testing local upload with {file_path}...")
    try:
        sample_file = client.files.upload(file=file_path)
        print(f"Uploaded: {sample_file.uri}")
        
        # Wait a bit for processing
        import time
        while sample_file.state.name == "PROCESSING":
            time.sleep(2)
            sample_file = client.files.get(name=sample_file.name)

        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=[
                types.Content(
                    role="user",
                    parts=[
                        types.Part.from_text(text="What is this?"),
                        types.Part.from_uri(file_uri=sample_file.uri, mime_type="application/pdf")
                    ]
                )
            ]
        )
        print("Success (Local Upload)!")
        print(response.text[:200])
    except Exception as e:
        print(f"FAILED (Local Upload): {e}")

if __name__ == "__main__":
    test_line()
