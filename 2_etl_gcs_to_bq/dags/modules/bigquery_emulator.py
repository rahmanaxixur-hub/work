import os
from pathlib import Path
import pandas as pd
import shutil

class BigQueryEmulator:
    """A simple BigQuery emulator that uses CSV files as tables or HTTP API if endpoint is set"""
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
        if gcs_uri.startswith("gs://"):
            parts = gcs_uri[5:].split("/")
            bucket_name = parts[0]
            blob_name = "/".join(parts[1:])
            gcs_base_path = Path(os.environ.get('GCS_BASE_PATH', '/tmp/airflow_gcs_emulator'))
            file_path = gcs_base_path / bucket_name / blob_name
            if file_path.exists():
                dataset_path = self.base_path / dataset_name
                dataset_path.mkdir(exist_ok=True, parents=True)
                table_path = dataset_path / f"{table_name}.csv"
                shutil.copy2(file_path, table_path)
                print(f"Loaded data from {gcs_uri} to {dataset_name}.{table_name}")
                return True
            else:
                print(f"File not found: {file_path}")
                return False
        return False

    def create_table_from_dataframe(self, dataset_name, table_name, dataframe):
        dataset_path = self.base_path / dataset_name
        dataset_path.mkdir(exist_ok=True, parents=True)
        table_path = dataset_path / f"{table_name}.csv"
        dataframe.to_csv(table_path, index=False)
        print(f"Created table {dataset_name}.{table_name} from DataFrame")
        return True

    def query(self, query, dataset_name, table_name):
        print(f"Executing query: {query}")
        table_path = self.base_path / dataset_name / f"{table_name}.csv"
        if table_path.exists():
            df = pd.read_csv(table_path)
            return df
        else:
            print(f"Table not found: {table_path}")
            return pd.DataFrame()

    def apply_udf_to_columns(self, dataset_name, table_name, columns, udf_function):
        table_path = self.base_path / dataset_name / f"{table_name}.csv"
        if table_path.exists():
            df = pd.read_csv(table_path)
            for col in columns:
                if col in df.columns:
                    df[col] = df[col].apply(udf_function)
            df.to_csv(table_path, index=False)
            print(f"Applied UDF to columns {columns} in {dataset_name}.{table_name}")
            return True
        else:
            print(f"Table not found: {table_path}")
            return False
