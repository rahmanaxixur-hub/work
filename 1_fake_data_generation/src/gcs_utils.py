import os
from google.cloud import storage

def upload_to_gcs(local_file_path, bucket_name, destination_blob_name, emulator_host=None):
    """
    Uploads a file to Google Cloud Storage. If emulator_host is provided, connects to the emulator.
    Creates the bucket if it does not exist.
    """
    if emulator_host:
        # Set environment variable for emulator
        os.environ["STORAGE_EMULATOR_HOST"] = emulator_host
        client = storage.Client(client_options={"api_endpoint": emulator_host})
    else:
        client = storage.Client()
    bucket = client.bucket(bucket_name)
    # Create the bucket if it does not exist
    if not bucket.exists():
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"Uploaded {local_file_path} to gs://{bucket_name}/{destination_blob_name}")
