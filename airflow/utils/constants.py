import configparser
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Create a configparser object
config = configparser.ConfigParser()

# Read the .conf file
config.read('airflow\config\secrets.conf')

# Access the secrets from the file
nasa_api_key = config['API']['nasa_api_key']
gcs_bucket = config['GCS']['gcs_bucket']
gcs_project_id = config['GCS']['gcs_project_id']
bigquery_project_id = config['BigQuery']['bigquery_project_id']
bigquery_dataset = config['BigQuery']['bigquery_dataset']

# Print the secrets (avoid printing sensitive data in real applications)
# print(f"NASA API Key: {nasa_api_key}")
# print(f"GCS Bucket: {gcs_bucket}")
