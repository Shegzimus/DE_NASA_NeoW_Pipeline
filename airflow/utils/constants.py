import configparser
import os

# Create a configparser object
secrets = configparser.ConfigParser()

# Read the .conf file
secrets.read('/opt/airflow/config/secrets.conf')


nasa_api_key = secrets.get('API', 'nasa_api_key')
