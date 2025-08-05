import os
import sys
import requests
from zipfile import ZipFile

# Step 1: Download the dataset from Kaggle 
def download_file(url, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "archive.zip")
    print(f"Downloading {url} to {filename}...")
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return filename

# Step 2: Extract the downloaded zip file
def extract_zip(zip_path, output_dir):
    print(f"Extracting {zip_path}...")
    with ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
    os.remove(zip_path)

# Step 3: Fix JSON format for PySpark
def fix_json(json_path):
    import json
    print(f"Fixing JSON file: {json_path}")
    with open(json_path, 'r') as f:
        data = json.load(f)

    fixed_path = json_path.replace('.json', '_fixed.json')
    with open(fixed_path, 'w') as out:
        for key, value in data.items():
            record = {key: value}
            out.write(json.dumps(record) + "\n")

    os.remove(json_path)
    os.rename(fixed_path, json_path)

# Step 4: Main execution
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract/execute.py <output_directory>")
        sys.exit(1)

    EXTRACT_PATH = sys.argv[1]
    # Kaggle URL (fixed)
    KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250803%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250803T083836Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=8477a52f492261cd89f3d9e5ac8c6f0481773fbaed0e44d8f0e014e0ff033dbd7cd12672d198233af1129de26b99634b0592330e07d457129db968918b84a59ce76a656946f59bf5eba8b79ad6b296b8381081bd3511d34badb3051f086d4bf49a36d270c14861197d1778b3cca7f9fb39fb9126a7a0514007e9eec199c5a7ff76b1245e59b2c6a359c633091cce89a913275418a673a061ad3d1591b7269a00d95de5c561e0b47efbd41e273dd7a7471ff05e9b15eaa1047b5e640ebf04c4b37c2fa9abc45f9442d2a7e65229598af00eb1f1c458b58e45a5253c160352bee7cb213f8e02db98a4d39fb323f0b1a9c21f636981e6b58f34948f2ce62765732a"

    try:
        zip_file = download_file(KAGGLE_URL, EXTRACT_PATH)
        extract_zip(zip_file, EXTRACT_PATH)

        # JSON filename fixed as 'da.json'
        json_path = os.path.join(EXTRACT_PATH, 'da.json')
        if os.path.exists(json_path):
            fix_json(json_path)

        print("Extraction completed successfully!")

    except Exception as e:
        print(f"Error: {e}")