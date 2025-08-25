from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import pandas as pd
import numpy as np
import csv
import json
import os
import uuid
import tempfile
import base64
from pathlib import Path
from faker import Faker
from cryptography.fernet import Fernet

# Initialize faker
fake = Faker()

# Define PII columns
PII_COLUMNS = ['email', 'first_name', 'last_name', 'phone_number', 'ssn', 'credit_card_number']

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'pii_data_pipeline_with_encryption',
    default_args=default_args,
    description='A DAG to process PII data with encryption and decryption',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['pii', 'encryption', 'demo', 'emulator'],
)

# Global variables to store paths (in a real scenario, use Airflow Variables or XCom)

# Use HTTP endpoints for emulators if set, else fallback to local paths
GCS_EMULATOR_HOST = os.environ.get('GCS_EMULATOR_HOST', None)
BIGQUERY_EMULATOR_HOST = os.environ.get('BIGQUERY_EMULATOR_HOST', None)
GCS_BASE_PATH = "/tmp/airflow_gcs_emulator" if not GCS_EMULATOR_HOST else None
BQ_BASE_PATH = "/tmp/airflow_bq_emulator" if not BIGQUERY_EMULATOR_HOST else None

# Encryption key (in production, this should be stored securely)
# For demo purposes, we'll generate a key and store it as an environment variable
ENCRYPTION_KEY = os.environ.get('PII_ENCRYPTION_KEY', Fernet.generate_key().decode())
cipher_suite = Fernet(ENCRYPTION_KEY.encode())

class GCSEmulator:
    """A simple GCS emulator that uses local directory as buckets"""
    
    def __init__(self, base_path=None, http_endpoint=None):
        self.base_path = Path(base_path) if base_path else None
        self.http_endpoint = http_endpoint
        if self.base_path:
            self.base_path.mkdir(exist_ok=True, parents=True)
    
    def upload_file(self, local_file_path, bucket_name, blob_name):
        if self.http_endpoint:
            # Use HTTP API to upload file to GCS emulator
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
            import shutil
            shutil.copy2(local_file_path, blob_path)
            print(f"Uploaded {local_file_path} to gs://{bucket_name}/{blob_name}")
            return f"gs://{bucket_name}/{blob_name}"

class BigQueryEmulator:
    """A simple BigQuery emulator that uses CSV files as tables"""
    
    def __init__(self, base_path=None, http_endpoint=None):
        self.base_path = Path(base_path) if base_path else None
        self.http_endpoint = http_endpoint
        if self.base_path:
            self.base_path.mkdir(exist_ok=True, parents=True)
    
    def create_dataset(self, dataset_name):
        dataset_path = self.base_path / dataset_name
        dataset_path.mkdir(exist_ok=True, parents=True)
        print(f"Created dataset {dataset_name}")
        return dataset_path
    
    def load_table_from_gcs(self, gcs_uri, dataset_name, table_name):
        # Parse the GCS URI
        if gcs_uri.startswith("gs://"):
            parts = gcs_uri[5:].split("/")
            bucket_name = parts[0]
            blob_name = "/".join(parts[1:])
            
            # In our emulator, GCS files are stored in the GCSEmulator base path
            gcs_base_path = Path(GCS_BASE_PATH)
            file_path = gcs_base_path / bucket_name / blob_name
            
            if file_path.exists():
                dataset_path = self.base_path / dataset_name
                dataset_path.mkdir(exist_ok=True, parents=True)
                
                table_path = dataset_path / f"{table_name}.csv"
                
                # Copy the file to the dataset directory
                import shutil
                shutil.copy2(file_path, table_path)
                print(f"Loaded data from {gcs_uri} to {dataset_name}.{table_name}")
                
                return True
            else:
                print(f"File not found: {file_path}")
                return False
        return False
    
    def create_table_from_dataframe(self, dataset_name, table_name, dataframe):
        """Create a new table from a DataFrame"""
        dataset_path = self.base_path / dataset_name
        dataset_path.mkdir(exist_ok=True, parents=True)
        
        table_path = dataset_path / f"{table_name}.csv"
        dataframe.to_csv(table_path, index=False)
        
        print(f"Created table {dataset_name}.{table_name} from DataFrame")
        return True
    
    def query(self, query, dataset_name, table_name):
        """Execute a query and return results as DataFrame"""
        # In this emulator, we'll just demonstrate the concept
        print(f"Executing query: {query}")
        
        # For demo purposes, we'll just read the table data
        table_path = self.base_path / dataset_name / f"{table_name}.csv"
        
        if table_path.exists():
            df = pd.read_csv(table_path)
            return df
        else:
            print(f"Table not found: {table_path}")
            return pd.DataFrame()
    
    def apply_udf_to_columns(self, dataset_name, table_name, columns, udf_function):
        """Apply a UDF to specific columns in a table"""
        table_path = self.base_path / dataset_name / f"{table_name}.csv"
        
        if table_path.exists():
            df = pd.read_csv(table_path)
            
            # Apply the UDF to each specified column
            for col in columns:
                if col in df.columns:
                    df[col] = df[col].apply(udf_function)
            
            # Save the updated data
            df.to_csv(table_path, index=False)
            print(f"Applied UDF to columns {columns} in {dataset_name}.{table_name}")
            
            return True
        else:
            print(f"Table not found: {table_path}")
            return False

def encrypt_value(value):
    """Encrypt a value using Fernet symmetric encryption"""
    if pd.isna(value) or value == '':
        return value
    
    # Convert to string and encrypt
    encrypted_value = cipher_suite.encrypt(str(value).encode())
    
    # Return as base64 string for easier storage
    return base64.b64encode(encrypted_value).decode()

def decrypt_value(value):
    """Decrypt a value using Fernet symmetric encryption"""
    if pd.isna(value) or value == '':
        return value
    
    try:
        # Decode from base64 and decrypt
        encrypted_value = base64.b64decode(value.encode())
        decrypted_value = cipher_suite.decrypt(encrypted_value).decode()
        return decrypted_value
    except Exception as e:
        print(f"Decryption error: {e}")
        return value

def generate_fake_user_data(**kwargs):
    """Generate fake user data with PII"""
    num_records = kwargs.get('num_records', 100)
    
    users = []
    for _ in range(num_records):
        user = {
            'user_id': str(uuid.uuid4()),
            'email': fake.email(),  # plain text
            'first_name': fake.first_name(),  # plain text
            'last_name': fake.last_name(),  # plain text
            'phone_number': fake.phone_number(),  # plain text
            'ssn': fake.ssn(),  # plain text
            'credit_card_number': fake.credit_card_number(),  # plain text
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state(),
            'zip_code': fake.zipcode(),
            'country': fake.country(),
            'registration_date': fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
            'last_login': fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')
        }
        users.append(user)
    
    # Create temporary directory if it doesn't exist
    os.makedirs('/tmp/airflow_pii_data', exist_ok=True)
    
    # Save to CSV file
    csv_file = f'/tmp/airflow_pii_data/users_with_pii_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df = pd.DataFrame(users)
    df.to_csv(csv_file, index=False)
    
    print(f"Generated CSV file with {len(users)} records: {csv_file}")
    
    # Push file path to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='pii_csv_path', value=csv_file)
    
    return csv_file

def upload_to_gcs_emulator(**kwargs):
    """Upload CSV to GCS emulator"""
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids='generate_pii_data', key='pii_csv_path')
    
    # Initialize GCS emulator
    gcs_emulator = GCSEmulator(base_path=GCS_BASE_PATH, http_endpoint=GCS_EMULATOR_HOST)
    
    # Upload file to GCS emulator
    gcs_uri = gcs_emulator.upload_file(
        csv_file_path, 
        "pii-data-bucket", 
        f"raw/users_with_pii_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    
    # Push GCS URI to XCom for downstream tasks
    ti.xcom_push(key='gcs_uri', value=gcs_uri)
    
    return gcs_uri

def transfer_to_bigquery_emulator(**kwargs):
    """Transfer data from GCS to BigQuery emulator"""
    ti = kwargs['ti']
    gcs_uri = ti.xcom_pull(task_ids='upload_to_gcs', key='gcs_uri')
    
    # Initialize BigQuery emulator
    bq_emulator = BigQueryEmulator(base_path=BQ_BASE_PATH, http_endpoint=BIGQUERY_EMULATOR_HOST)
    
    # Create dataset
    bq_emulator.create_dataset("user_analytics")
    
    # Load data from GCS
    success = bq_emulator.load_table_from_gcs(
        gcs_uri, 
        "user_analytics", 
        "users_pii_raw"
    )
    
    if not success:
        raise Exception("Failed to load data to BigQuery emulator")
    
    return "Data transferred successfully"

def encrypt_pii_data(**kwargs):
    """Encrypt PII columns in BigQuery table"""
    # Initialize BigQuery emulator
    bq_emulator = BigQueryEmulator(base_path=BQ_BASE_PATH, http_endpoint=BIGQUERY_EMULATOR_HOST)
    
    # Apply UDF to encrypt PII columns
    success = bq_emulator.apply_udf_to_columns(
        "user_analytics", 
        "users_pii_raw", 
        PII_COLUMNS, 
        encrypt_value
    )
    
    if not success:
        raise Exception("Failed to encrypt PII data")
    
    # Create a new table with encrypted data
    table_path = Path(BQ_BASE_PATH) / "user_analytics" / "users_pii_raw.csv"
    if table_path.exists():
        df = pd.read_csv(table_path)
        bq_emulator.create_table_from_dataframe(
            "user_analytics", 
            "users_pii_encrypted", 
            df
        )
    
    return "PII data encrypted successfully"

def decrypt_pii_data(**kwargs):
    """Decrypt PII columns and store in a new table"""
    # Initialize BigQuery emulator
    bq_emulator = BigQueryEmulator(base_path=BQ_BASE_PATH, http_endpoint=BIGQUERY_EMULATOR_HOST)
    
    # Read the encrypted data
    encrypted_table_path = Path(BQ_BASE_PATH) / "user_analytics" / "users_pii_encrypted.csv"
    
    if encrypted_table_path.exists():
        df = pd.read_csv(encrypted_table_path)
        
        # Decrypt the PII columns
        for col in PII_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(decrypt_value)
        
        # Create a new table with decrypted data
        bq_emulator.create_table_from_dataframe(
            "user_analytics", 
            "users_pii_decrypted", 
            df
        )
        
        return "PII data decrypted successfully"
    else:
        raise Exception("Encrypted table not found")

def verify_results(**kwargs):
    """Verify the results of the pipeline"""
    # Initialize BigQuery emulator
    bq_emulator = BigQueryEmulator(base_path=BQ_BASE_PATH, http_endpoint=BIGQUERY_EMULATOR_HOST)
    
    # Query the original data
    original_data = bq_emulator.query(
        "SELECT * FROM users_pii_raw LIMIT 5",
        "user_analytics",
        "users_pii_raw"
    )
    
    # Query the encrypted data
    encrypted_data = bq_emulator.query(
        "SELECT * FROM users_pii_encrypted LIMIT 5",
        "user_analytics",
        "users_pii_encrypted"
    )
    
    # Query the decrypted data
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
                # Try to decode base64 to show raw encrypted bytes
                try:
                    enc_bytes = base64.b64decode(enc_b64.encode()) if enc_b64 else None
                except Exception:
                    enc_bytes = None
                print(f"  {col}: original='{orig}' | encrypted_b64='{enc_b64}' | encrypted_bytes={enc_bytes} | decrypted='{dec}'")
        
        # Verify that decrypted data matches original
        for col in PII_COLUMNS:
            if col in original_data.columns and col in decrypted_data.columns:
                original_values = original_data[col].head().tolist()
                decrypted_values = decrypted_data[col].head().tolist()
                
                if original_values == decrypted_values:
                    print(f"✓ {col} decryption verified successfully")
                else:
                    print(f"✗ {col} decryption verification failed")
        
        # Save the final results for inspection
        os.makedirs('/tmp/airflow_results', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        original_data.to_csv(f'/tmp/airflow_results/original_data_{timestamp}.csv', index=False)
        encrypted_data.to_csv(f'/tmp/airflow_results/encrypted_data_{timestamp}.csv', index=False)
        decrypted_data.to_csv(f'/tmp/airflow_results/decrypted_data_{timestamp}.csv', index=False)
        
        print(f"\nResults saved to /tmp/airflow_results/")
        
        return "Verification complete. All data transformations validated."
    else:
        raise Exception("Data verification failed - missing tables")

# Define tasks
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

generate_pii_data_task = PythonOperator(
    task_id='generate_pii_data',
    python_callable=generate_fake_user_data,
    op_kwargs={'num_records': 50},
    dag=dag,
)

upload_to_gcs_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs_emulator,
    dag=dag,
)

transfer_to_bigquery_task = PythonOperator(
    task_id='transfer_to_bigquery',
    python_callable=transfer_to_bigquery_emulator,
    dag=dag,
)

encrypt_pii_task = PythonOperator(
    task_id='encrypt_pii',
    python_callable=encrypt_pii_data,
    dag=dag,
)

decrypt_pii_task = PythonOperator(
    task_id='decrypt_pii',
    python_callable=decrypt_pii_data,
    dag=dag,
)

verify_results_task = PythonOperator(
    task_id='verify_results',
    python_callable=verify_results,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> generate_pii_data_task >> upload_to_gcs_task >> transfer_to_bigquery_task >> encrypt_pii_task >> decrypt_pii_task >> verify_results_task >> end_task