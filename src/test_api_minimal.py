import os
import time
from google import genai
from google.genai import types

API_KEY = os.getenv("GoogleAI-API-key")
client = genai.Client(api_key=API_KEY)

# Need a local file for the File API upload
LOCAL_PDF = r"C:\MyDocs\sandbox\build_26R_library\data\corpus\003297_Edsell Comp_470 Inlet 2021.pdf"

# If not exists, I'll have to skip this for a moment or find one.
if not os.path.exists(LOCAL_PDF):
    print(f"Local file {LOCAL_PDF} not found. Searching for any PDF...")
    import glob
    pdfs = glob.glob("**/*.pdf", recursive=True)
    if pdfs:
        LOCAL_PDF = pdfs[0]
        print(f"Using {LOCAL_PDF}")
    else:
        print("No local PDF found.")
        exit(1)

try:
    print(f"Uploading {LOCAL_PDF}...")
    file = client.files.upload(file=LOCAL_PDF)
    print(f"Uploaded as {file.uri}")

    while file.state.name == "PROCESSING":
        time.sleep(2)
        file = client.files.get(name=file.name)
    
    if file.state.name == "FAILED":
        print("File processing failed.")
        exit(1)

    response = client.models.generate_content(
        model="gemini-3-flash-preview", 
        contents=[
            file,
            "What is this document?"
        ]
    )
    print(f"Success: {response.text}")
except Exception as e:
    print(f"Failed: {e}")
