from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from google.cloud import storage, bigquery
import os

# Set your variables here or use Airflow Variables/Connections
GCS_BUCKET = os.getenv('GCS_BUCKET', 'your-bucket')
GCS_FILE = os.getenv('GCS_FILE', 'your-file.csv')
BQ_PROJECT = os.getenv('GCP_PROJECT', 'your-gcp-project')
BQ_DATASET = os.getenv('BQ_DATASET', 'your_dataset')
BQ_TABLE = os.getenv('BQ_TABLE', 'your_table')


default_args = {
    'owner': 'airflow',
}

def extract_from_gcs():
    # No need to download, just return the GCS URI
    return f"gs://{GCS_BUCKET}/{GCS_FILE}"


def load_to_bigquery(local_path):
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    # local_path is actually the GCS URI now
    job = client.load_table_from_uri(local_path, table_id, job_config=job_config)
    job.result()
    return f"Loaded {local_path} to {table_id}"


define_dag = DAG(
    dag_id="gcs_to_bq_etl",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

with define_dag:
    @task()
    def extract_task():
        return extract_from_gcs()

    @task()
    def load_task(local_path: str):
        return load_to_bigquery(local_path)

    extract_task_output = extract_task()
    load_task(extract_task_output)
