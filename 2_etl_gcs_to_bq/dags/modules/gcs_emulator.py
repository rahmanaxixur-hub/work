import os
from pathlib import Path
import shutil

class GCSEmulator:
    """A simple GCS emulator that uses local directory as buckets or HTTP API if endpoint is set"""
    def __init__(self, base_path=None, http_endpoint=None):
        self.base_path = Path(base_path) if base_path else None
        self.http_endpoint = http_endpoint
        if self.base_path:
            self.base_path.mkdir(exist_ok=True, parents=True)

    def upload_file(self, local_file_path, bucket_name, blob_name):
        if self.http_endpoint:
            import requests
            url = f"{self.http_endpoint}/upload/storage/v1/b/{bucket_name}/o?uploadType=media&name={blob_name}"
            with open(local_file_path, 'rb') as f:
                resp = requests.post(url, data=f)
            if resp.status_code in (200, 201):
                print(f"Uploaded {local_file_path} to gs://{bucket_name}/{blob_name} via HTTP emulator")
                return f"gs://{bucket_name}/{blob_name}"
            else:
                print(f"Failed to upload to GCS emulator: {resp.status_code} {resp.text}")
                raise Exception("GCS upload failed")
        else:
            bucket_path = self.base_path / bucket_name
            bucket_path.mkdir(exist_ok=True, parents=True)
            blob_path = bucket_path / blob_name
            blob_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_file_path, blob_path)
            print(f"Uploaded {local_file_path} to gs://{bucket_name}/{blob_name}")
            return f"gs://{bucket_name}/{blob_name}"
