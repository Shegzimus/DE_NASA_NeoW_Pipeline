import configparser
import os

# Create a configparser object
secrets = configparser.ConfigParser()

# Read the .conf file
secrets.read('/opt/airflow/config/secrets.conf')
# config.read(os.path.join(os.path.dirname(__file__), '../config/secrets.conf'))

nasa_api_key = secrets.get('API', 'nasa_api_key')
# # Access the secrets from the file
# try:
#     with open('/run/secrets/api_key_config', 'r') as secret_file:
#         nasa_api_key = secret_file.read().strip()
# except FileNotFoundError:
#     raise RuntimeError("The API key secret file was not found.")
