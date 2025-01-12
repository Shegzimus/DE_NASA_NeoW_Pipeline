from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig
import os 
import sys


def execute_sql_from_file(sql_file_path: str, destination_table: str, disposition: str) -> None:
    """Reads and executes a SQL query from a directory file using BigQuery"""
    
    check_if_file_exists(sql_file_path)

    client = initialize_bigquery()

    query = read_sql_from_file(sql_file_path)

    job_config = configure_job(disposition, destination_table)

    # Execute the query
    query_job = client.query(query, job_config= job_config)
    query_job.result()  # Wait for the query to complete
    print(f"Query executed from {sql_file_path}")


def configure_job(disposition: str, destination_table: str) -> QueryJobConfig:
    """Configures and returns a QueryJobConfig object based on the disposition."""
    # Set the write disposition based on the provided disposition parameter.
    # Default is 'WRITE_TRUNCATE' to overwrite the table.
    
    if disposition in ('overwrite', ''):
        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition='WRITE_TRUNCATE'  # Overwrite the table
        )
    elif disposition == 'append':
        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition='WRITE_APPEND'  # Append to the table
        )
    elif disposition == 'empty':
        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition='WRITE_EMPTY'  # Only write if table is empty
        )
    else:
        raise ValueError(f"Invalid disposition: {disposition}. Only overwrite, append and empty is allowed.")

    return job_config

def check_if_file_exists(sql_file_path: str) -> None:
    if not os.path.isfile(sql_file_path):
        print(f"Error: {sql_file_path} does not exist. Please ensure the file exists and is accessible.")
        sys.exit(1)

def initialize_bigquery() -> None:
    """Initializes a BigQuery client"""
    client = bigquery.Client()
    return client

def read_sql_from_file(sql_file_path: str) -> None:
    """Reads and returns a SQL query from a file."""
    with open(sql_file_path, 'r') as file:
        return file.read()










if __name__ == '__main__':
    pass
