
import os
from datetime import datetime
from fake_user_generator import generate_users, get_user_fields
from csv_utils import save_users_to_csv
from gcs_utils import upload_to_gcs


def main():
    output_dir = os.getenv('FAKE_USER_DATA_DIR')
    if not output_dir:
        output_dir = os.path.join(os.path.dirname(__file__), '..', 'fake_user_data')

    # Create output filename with date-time
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"fake_users_{timestamp}.csv"

    # Get number of users from environment variable or default
    try:
        num_users = int(os.getenv('NUM_USERS', '1000'))
    except ValueError:
        num_users = 1000

    # Generate users
    users = generate_users(num_users)

    # Save users to CSV
    fields = get_user_fields()
    local_file_path = save_users_to_csv(users, filename, output_dir, fields)

    # Upload the generated file to GCS (emulator or real)
    emulator_host = os.getenv('GCS_EMULATOR_HOST', 'http://localhost:4443')
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'test-bucket')
    destination_blob_name = filename
    try:
        upload_to_gcs(local_file_path, bucket_name, destination_blob_name, emulator_host)
    except Exception as e:
        print(f"Failed to upload to GCS: {e}")

if __name__ == "__main__":
    main()
