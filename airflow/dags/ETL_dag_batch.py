import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Add the parent directory to the system path for importing the contants file
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import nasa_api_key

from pipelines.extract import (generate_time_range,
                               test_api_call,
                               extract_batch_close_approach,
                               extract_batch_neo_data_raw)

from pipelines.transform import (transform_batch_close_approach,
                                 transform_neo_feed_batch)

from pipelines.load import upload_folder_to_gcs



API_KEY = nasa_api_key
today_date = datetime.now().date()
start_date, end_date, postfix = generate_time_range(today_date)
BUCKET_NAME = 'de_data_lake_nasa-neows'

# Define the DAG and default args
default_args = {
    'owner': 'Oluwasegun',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 8, 1, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='Batch_ETL',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    max_active_runs=1,
    tags=['Batch','NASA', 'Near_Earth_Objects']
)

"""
Task 1: Test the API

"""

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
extract_batch_close_approach_task = PythonOperator(
    task_id='extract_close_approach',
    python_callable=extract_batch_close_approach(today_date),
    dag=dag
)

"""

Task 3: Extract the Neo data raw

"""

extract_batch_neo_feed_task = PythonOperator(
    task_id='extract_neo_data_raw',
    python_callable=extract_batch_neo_data_raw(today_date),
    dag=dag   
    )


"""
Task 4: Transform the close approach data

"""

transform_close_approach_task = PythonOperator(
    task_id='transform_close_approach',
    python_callable=transform_batch_close_approach(today_date),
    dag=dag
)



"""
Task 5: Transform the Neo data feed

"""
transform_batch_neo_feed_task = PythonOperator(
    task_id='transform_neo_feed',
    python_callable=transform_neo_feed_batch(today_date),
    dag=dag
)


"""
Task 6: Upload the Close approach data

"""

upload_batch_close_approach_task = PythonOperator(
    task_id='upload_batch_close_approach',
    python_callable=upload_folder_to_gcs(
        bucket_name=BUCKET_NAME,
        local_folder="opt/airflow/data/output/batch/close_approach",
        target_folder_prefix='batch/close_approach/'
    ),
    dag=dag
)

"""
Task 7: Upload the Neo feed data

"""
upload_batch_neo_feed_task =PythonOperator(
    task_id='upload_batch_close_approach',
    python_callable=upload_folder_to_gcs(
        bucket_name=BUCKET_NAME,
        local_folder="opt/airflow/data/output/batch/neo_feed",
        target_folder_prefix='batch/neo_feed/'
    ),
    dag=dag
)


Begin = DummyOperator(task_id="Begin", dag=dag)
End = DummyOperator(task_id="End", dag=dag)


Begin >> test_api_task  >> [extract_batch_close_approach_task, extract_batch_neo_feed_task]

extract_batch_close_approach_task >> transform_close_approach_task >> upload_batch_close_approach_task
extract_batch_neo_feed_task >> transform_batch_neo_feed_task >> upload_batch_neo_feed_task

[upload_batch_close_approach_task, upload_batch_neo_feed_task] >> End
