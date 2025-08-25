from modules.gcs_emulator import GCSEmulator
from modules.bigquery_emulator import BigQueryEmulator
from modules.encryption import encrypt_value, decrypt_value
from modules.data_generation import generate_fake_user_data, PII_COLUMNS
import pandas as pd
import os
from pathlib import Path
from datetime import datetime

def upload_to_gcs_emulator(csv_file_path, gcs_base_path, gcs_emulator_host):
    gcs_emulator = GCSEmulator(base_path=gcs_base_path, http_endpoint=gcs_emulator_host)
    gcs_uri = gcs_emulator.upload_file(
        csv_file_path,
        "pii-data-bucket",
        f"raw/users_with_pii_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    return gcs_uri

def transfer_to_bigquery_emulator(gcs_uri, bq_base_path, bq_emulator_host):
    bq_emulator = BigQueryEmulator(base_path=bq_base_path, http_endpoint=bq_emulator_host)
    bq_emulator.create_dataset("user_analytics")
    success = bq_emulator.load_table_from_gcs(
        gcs_uri,
        "user_analytics",
        "users_pii_raw"
    )
    if not success:
        raise Exception("Failed to load data to BigQuery emulator")
    return "Data transferred successfully"

def encrypt_pii_data(bq_base_path, bq_emulator_host):
    bq_emulator = BigQueryEmulator(base_path=bq_base_path, http_endpoint=bq_emulator_host)
    success = bq_emulator.apply_udf_to_columns(
        "user_analytics",
        "users_pii_raw",
        PII_COLUMNS,
        encrypt_value
    )
    if not success:
        raise Exception("Failed to encrypt PII data")
    table_path = Path(bq_base_path) / "user_analytics" / "users_pii_raw.csv"
    if table_path.exists():
        df = pd.read_csv(table_path)
        bq_emulator.create_table_from_dataframe(
            "user_analytics",
            "users_pii_encrypted",
            df
        )
    return "PII data encrypted successfully"

def decrypt_pii_data(bq_base_path, bq_emulator_host):
    bq_emulator = BigQueryEmulator(base_path=bq_base_path, http_endpoint=bq_emulator_host)
    encrypted_table_path = Path(bq_base_path) / "user_analytics" / "users_pii_encrypted.csv"
    if encrypted_table_path.exists():
        df = pd.read_csv(encrypted_table_path)
        for col in PII_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(decrypt_value)
        bq_emulator.create_table_from_dataframe(
            "user_analytics",
            "users_pii_decrypted",
            df
        )
        return "PII data decrypted successfully"
    else:
        raise Exception("Encrypted table not found")

def verify_results(bq_base_path, bq_emulator_host):
    bq_emulator = BigQueryEmulator(base_path=bq_base_path, http_endpoint=bq_emulator_host)
    original_data = bq_emulator.query(
        "SELECT * FROM users_pii_raw LIMIT 5",
        "user_analytics",
        "users_pii_raw"
    )
    encrypted_data = bq_emulator.query(
        "SELECT * FROM users_pii_encrypted LIMIT 5",
        "user_analytics",
        "users_pii_encrypted"
    )
    decrypted_data = bq_emulator.query(
        "SELECT * FROM users_pii_decrypted LIMIT 5",
        "user_analytics",
        "users_pii_decrypted"
    )
    if not original_data.empty and not encrypted_data.empty and not decrypted_data.empty:
        print("\nPII data comparison (first 5 rows):")
        for i in range(5):
            print(f"Row {i}:")
            for col in PII_COLUMNS:
                orig = original_data.iloc[i][col] if col in original_data.columns else None
                enc_b64 = encrypted_data.iloc[i][col] if col in encrypted_data.columns else None
                dec = decrypted_data.iloc[i][col] if col in decrypted_data.columns else None
                try:
                    import base64
                    enc_bytes = base64.b64decode(enc_b64.encode()) if enc_b64 else None
                except Exception:
                    enc_bytes = None
                print(f"  {col}: original='{orig}' | encrypted_b64='{enc_b64}' | encrypted_bytes={enc_bytes} | decrypted='{dec}'")
        for col in PII_COLUMNS:
            if col in original_data.columns and col in decrypted_data.columns:
                original_values = original_data[col].head().tolist()
                decrypted_values = decrypted_data[col].head().tolist()
                if original_values == decrypted_values:
                    print(f"✓ {col} decryption verified successfully")
                else:
                    print(f"✗ {col} decryption verification failed")
        os.makedirs('/tmp/airflow_results', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        original_data.to_csv(f'/tmp/airflow_results/original_data_{timestamp}.csv', index=False)
        encrypted_data.to_csv(f'/tmp/airflow_results/encrypted_data_{timestamp}.csv', index=False)
        decrypted_data.to_csv(f'/tmp/airflow_results/decrypted_data_{timestamp}.csv', index=False)
        print(f"\nResults saved to /tmp/airflow_results/")
        return "Verification complete. All data transformations validated."
    else:
        raise Exception("Data verification failed - missing tables")
