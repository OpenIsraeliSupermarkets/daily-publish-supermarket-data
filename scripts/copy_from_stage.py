import os
import tempfile
from datetime import datetime
from remotes.long_term.kaggle import KaggleUploader

# Set dataset names
SRC_DATASET = "test-super-dataset-2"
DST_DATASET = "israeli-supermarkets-2024"

# Use a temp directory for staging
with tempfile.TemporaryDirectory() as temp_dir:
    # Download from source
    src_uploader = KaggleUploader(temp_dir, SRC_DATASET, datetime.now())
    files = src_uploader.list_files()
    print(f"Found {len(files)} files in source dataset.")
    print("Downloading all files...")
    src_uploader.api.dataset_download_cli(f"erlichsefi/{SRC_DATASET}", force=True, path=temp_dir)

    # Unzip all downloaded files
    for file in os.listdir(temp_dir):
        if file.endswith('.zip'):
            import zipfile
            zip_path = os.path.join(temp_dir, file)
            print(f"Extracting {zip_path}...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            os.remove(zip_path)

    # Upload to destination
    dst_uploader = KaggleUploader(temp_dir, DST_DATASET, datetime.now())
    print(f"Uploading all files to {DST_DATASET}...")
    dst_uploader.increase_index()
    dst_uploader.upload_to_dataset(f"Back-up copy from {SRC_DATASET} on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Upload complete.")
