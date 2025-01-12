from google.cloud import bigquery
import os 
import sys


def execute_sql_from_file(sql_file_path):
    """Reads and executes a SQL query from a file using BigQuery."""
    # Initialize BigQuery client
    client = initialize_bigquery()

    # Read the SQL query from the file
    query = read_sql_from_file(sql_file_path)

    # Execute the query
    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete
    print(f"Query executed from {sql_file_path}")


def initialize_bigquery():
    """Initializes a BigQuery client."""
    client = bigquery.Client()
    return client

def read_sql_from_file(sql_file_path):
    """Reads and returns a SQL query from a file."""
    try:
        with open(sql_file_path, 'r') as file:
            return file.read()
    except:
        print(f"Error reading SQL query from {sql_file_path}. Please ensure the file exists and is accessible.")
        sys.exit(1)









if __name__ == '__main__':
    pass
