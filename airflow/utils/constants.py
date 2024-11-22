import configparser
import sys
import os

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

# URLs
# neo_feed =f'https://api.nasa.gov/neo/rest/v1/feed?start_date={START_DATE}&end_date={END_DATE}&api_key={API_KEY}'
# neo_lookup = f'https://api.nasa.gov/neo/rest/v1/neo/{ASTEROID_ID}?api_key={API_KEY}'
# neo_browse = f'https://api.nasa.gov/neo/rest/v1/neo/browse?api_key={API_KEY}'

# Paths for testing
LOCAL_OUTPUT_PATH = 'airflow/data/output'

LOCAL_INPUT_PATH = 'airflow/data/input'

# Paths for production 

OUTPUT_PATH = config['file_paths']['output_path']

INPUT_PATH = config['file_paths']['input_path']