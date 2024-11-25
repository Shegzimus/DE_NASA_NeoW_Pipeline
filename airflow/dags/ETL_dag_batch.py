import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Add the parent directory to the system path for importing the contants file
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import (nasa_api_key, 
                             OUTPUT_PATH, 
                             INPUT_PATH)

from pipelines.extract import (generate_time_range,
                               test_api_call,
                               extract_close_approach_column,
                               extract_neo_data_raw)


API_KEY = nasa_api_key
today_date = datetime.now().strftime("%Y-%m-%d")
start_date, end_date, postfix = generate_time_range(today_date)


# Define the DAG and default args
default_args = {
    'owner': 'Oluwasegun',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 22),
    'retries': 1,
}

dag = DAG(
    dag_id='Batch_Extract',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    max_active_runs=1,
    tags=['Batch','NASA', 'Near_Earth_Objects']
)

"""
Task 1: Test the API

"""

# test_api_task = PythonOperator(
#     task_id='test_api',
#     python_callable=lambda: os.system(f'curl -X GET f"https://api.nasa.gov/neo/rest/v1/feed?start_date={test_start}&end_date={test_end}&api_key={nasa_api_key}"'),
#     dag=dag
# )

test_api_task = PythonOperator(
    task_id='test_api',
    python_callable=test_api_call,
    op_kwargs={
        'START_DATE': start_date,
        'END_DATE': end_date,
        'API_KEY': nasa_api_key
    },
    dag=dag
)


"""

Task 2: Extract the close approach data

"""
extract_close_approach_task = PythonOperator(
    task_id='extract_close_approach',
    python_callable=extract_close_approach_column(today_date),
    dag=dag
)

"""

Task 3: Extract the Neo data raw

"""

extract_neo_data_raw_task = PythonOperator(
    task_id='extract_neo_data_raw',
    python_callable=extract_neo_data_raw(today_date),
    dag=dag   
    )