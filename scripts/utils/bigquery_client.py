"""
BigQuery client utilities and connection helpers
"""
import os
import pandas as pd
from google.cloud import bigquery
from typing import Optional

def get_bigquery_client(project_id: Optional[str] = None) -> bigquery.Client:
    """Get authenticated BigQuery client"""
    project_id = project_id or os.environ.get("BQ_PROJECT")
    return bigquery.Client(project=project_id)

def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    """Ensure dataset exists, create if not"""
    try:
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(dataset_id, exists_ok=True)
        print(f"Created dataset: {dataset_id}")

def load_dataframe_to_bq(df: pd.DataFrame, table_id: str, 
                        write_disposition: str = "WRITE_APPEND") -> bigquery.LoadJob:
    """Load pandas DataFrame to BigQuery table"""
    client = get_bigquery_client()
    
    # Extract dataset from table_id and ensure it exists
    project, dataset, table = table_id.split('.')
    ensure_dataset(client, f"{project}.{dataset}")
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        create_disposition="CREATE_IF_NEEDED"
    )
    
    # Load the data
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion
    
    print(f"Loaded {len(df)} rows into {table_id}")
    return job

def run_query(query: str, project_id: Optional[str] = None) -> pd.DataFrame:
    """Execute SQL query and return results as DataFrame"""
    client = get_bigquery_client(project_id)
    return client.query(query).to_dataframe()

def create_table_from_query(query: str, destination_table: str, 
                           write_disposition: str = "WRITE_TRUNCATE",
                           project_id: Optional[str] = None) -> bigquery.QueryJob:
    """Create/replace table from SQL query"""
    client = get_bigquery_client(project_id)
    
    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition=write_disposition,
        create_disposition="CREATE_IF_NEEDED"
    )
    
    job = client.query(query, job_config=job_config)
    job.result()  # Wait for completion
    
    print(f"Created table {destination_table} from query")
    return job