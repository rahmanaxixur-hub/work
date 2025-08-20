import os
from google.cloud import storage

def list_gcs_files(bucket_name, emulator_host=None):
    """
    List all files in the specified GCS bucket. Supports emulator.
    """
    if emulator_host:
        os.environ["STORAGE_EMULATOR_HOST"] = emulator_host
        client = storage.Client(client_options={"api_endpoint": emulator_host})
    else:
        client = storage.Client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        print(f"Bucket '{bucket_name}' does not exist.")
        return
    blobs = bucket.list_blobs()
    print(f"Files in bucket '{bucket_name}':")
    for blob in blobs:
        print(blob.name)

def main():
    emulator_host = os.getenv('GCS_EMULATOR_HOST', 'http://localhost:4443')
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'test-bucket')
    list_gcs_files(bucket_name, emulator_host)

if __name__ == "__main__":
    main()
