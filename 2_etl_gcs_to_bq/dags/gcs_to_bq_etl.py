from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
from modules.pipeline_tasks import (
    generate_fake_user_data, 
    upload_to_gcs_emulator, 
    transfer_to_bigquery_emulator,
    encrypt_pii_data, 
    decrypt_pii_data, 
    verify_results
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pii_data_pipeline_with_encryption',
    default_args=default_args,
    description='A DAG to process PII data with encryption and decryption',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['pii', 'encryption', 'demo', 'emulator'],
)

GCS_EMULATOR_HOST = os.environ.get('GCS_EMULATOR_HOST', None)
BIGQUERY_EMULATOR_HOST = os.environ.get('BIGQUERY_EMULATOR_HOST', None)
GCS_BASE_PATH = "/tmp/airflow_gcs_emulator" if not GCS_EMULATOR_HOST else None
BQ_BASE_PATH = "/tmp/airflow_bq_emulator" if not BIGQUERY_EMULATOR_HOST else None

def airflow_generate_pii_data(**kwargs):
    csv_file = generate_fake_user_data(num_records=20)
    kwargs['ti'].xcom_push(key='pii_csv_path', value=csv_file)
    return csv_file

def airflow_upload_to_gcs(**kwargs):
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids='generate_pii_data', key='pii_csv_path')
    gcs_uri = upload_to_gcs_emulator(csv_file_path, GCS_BASE_PATH, GCS_EMULATOR_HOST)
    ti.xcom_push(key='gcs_uri', value=gcs_uri)
    return gcs_uri

def airflow_transfer_to_bigquery(**kwargs):
    ti = kwargs['ti']
    gcs_uri = ti.xcom_pull(task_ids='upload_to_gcs', key='gcs_uri')
    return transfer_to_bigquery_emulator(gcs_uri, BQ_BASE_PATH, BIGQUERY_EMULATOR_HOST)

def airflow_encrypt_pii(**kwargs):
    return encrypt_pii_data(BQ_BASE_PATH, BIGQUERY_EMULATOR_HOST)

def airflow_decrypt_pii(**kwargs):
    return decrypt_pii_data(BQ_BASE_PATH, BIGQUERY_EMULATOR_HOST)

def airflow_verify_results(**kwargs):
    return verify_results(BQ_BASE_PATH, BIGQUERY_EMULATOR_HOST)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

generate_pii_data_task = PythonOperator(
    task_id='generate_pii_data',
    python_callable=airflow_generate_pii_data,
    dag=dag,
)

upload_to_gcs_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=airflow_upload_to_gcs,
    dag=dag,
)

transfer_to_bigquery_task = PythonOperator(
    task_id='transfer_to_bigquery',
    python_callable=airflow_transfer_to_bigquery,
    dag=dag,
)

encrypt_pii_task = PythonOperator(
    task_id='encrypt_pii',
    python_callable=airflow_encrypt_pii,
    dag=dag,
)

decrypt_pii_task = PythonOperator(
    task_id='decrypt_pii',
    python_callable=airflow_decrypt_pii,
    dag=dag,
)

verify_results_task = PythonOperator(
    task_id='verify_results',
    python_callable=airflow_verify_results,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

start_task >> generate_pii_data_task >> upload_to_gcs_task >> transfer_to_bigquery_task >> encrypt_pii_task >> decrypt_pii_task >> verify_results_task >> end_task