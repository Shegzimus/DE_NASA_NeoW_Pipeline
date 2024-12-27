import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Add the parent directory to the system path for importing the contants file
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import nasa_api_key

from pipelines.extract import (test_api_call,
                               generate_time_range,
                               extract_hist_neo_data_raw,
                               extract_hist_close_approach,
                               extract_and_save_ast_data)

from pipelines.transform import (process_hist_neo_feed_in_folder,
                                 process_hist_approach_in_folder,
                                 transform_hist_asteroid_raw)



from pipelines.load import upload_folder_to_gcs, extract_schema_from_parquet



API_KEY = nasa_api_key
today_date = datetime.now().strftime("%Y-%m-%d")
start_date, end_date, postfix = generate_time_range(today_date)

# Retrieve variables from Airflow environment
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
STAGING = os.environ.get("BQ_DATASET_STAGING")
WAREHOUSE = os.environ.get("BQ_DATASET_WAREHOUSE")

# Define the DAG and default args
default_args = {
    'owner': 'Oluwasegun',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 22),
    'retries': 1,
}

dag = DAG(
    dag_id='Historic_ETL',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['Historic','NASA', 'Near_Earth_Objects']
)


############################################################################################

"""
Task 1.0: Test the API

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
Task 1.1: Extract historical close approach data

"""
extract_hist_close_approach_task = PythonOperator(
    task_id='extract_close_approach',
    python_callable=extract_hist_close_approach("20200101"),
    dag=dag
)

"""
Task 1.2: Extract historical Neo data raw

"""

extract_hist_neo_data_task = PythonOperator(
    task_id='extract_neo_data_raw',
    python_callable=extract_hist_neo_data_raw("20200101"),
    dag=dag   
    )



"""
Task 1.3: Extract and save all Asteroid data

"""
extract_and_save_ast_data_task= PythonOperator(
    task_id='extract_and_save_ast_data',
    python_callable=extract_and_save_ast_data,
    dag=dag      
)


############################################################################################

"""
Task 2.0: Transform the neo feed data

"""

process_hist_neo_feed_in_folder_task = PythonOperator(
    task_id='process_hist_neo_feed_in_folder',
    python_callable=process_hist_neo_feed_in_folder,
    dag=dag
)

"""
Task 2.1: Transform the close approach data

"""

process_hist_approach_in_folder_task = PythonOperator(
    task_id='process_hist_approach_in_folder',
    python_callable=process_hist_approach_in_folder,
    dag=dag
)


"""
Task 2.2: Transform the historical asteroid data

"""

transform_hist_asteroid_raw_task = PythonOperator(
    task_id='transform_hist_asteroid_raw',
    python_callable=transform_hist_asteroid_raw,
    dag=dag
)


############################################################################################

"""
Task 3.0: Define the folder paths and their GCS target prefix

"""

bucket_name = BUCKET
folder_paths = [
    {"local_folder": "opt/airflow/data/output/historical/asteroid_data", "gcs_prefix": "historical/asteroid_data/"},
    {"local_folder": "opt/airflow/data/output/historical/close_approach", "gcs_prefix": "historical/close_approach/"},
    {"local_folder": "opt/airflow/data/output/historical/neo_feed", "gcs_prefix": "historical/neo_feed/"}
]



"""
Task 3.1: Upload the specified folders to the GCS Bucket

"""
upload_to_gcs_tasks = []
for folder in folder_paths:
    task = PythonOperator(
        task_id=f"upload_{os.path.basename(folder['local_folder'])}_to_gcs",
        python_callable=upload_folder_to_gcs,
        op_kwargs={
            "bucket_name": bucket_name,
            "local_folder": folder["local_folder"],
            "target_folder_prefix": folder["gcs_prefix"]
        },
        provide_context=True,
        dag=dag
    )
    upload_to_gcs_tasks.append(task)








Begin = DummyOperator(task_id="begin", dag=dag)

Extraction_Complete = DummyOperator(task_id="Extraction_Complete", dag=dag)

Transformation_Complete = DummyOperator(task_id="Transformation_Complete", dag=dag)

Load_Complete = DummyOperator(task_id="Load_Complete", dag=dag)

End = DummyOperator(task_id="end", dag=dag)


# Set task dependencies

Begin >> test_api_task

test_api_task >> extract_hist_close_approach_task

extract_hist_close_approach_task >> extract_hist_neo_data_task >> extract_and_save_ast_data_task >> Extraction_Complete


Extraction_Complete >> process_hist_neo_feed_in_folder_task >> process_hist_approach_in_folder_task >> transform_hist_asteroid_raw_task >> Transformation_Complete


Transformation_Complete >>  upload_to_gcs_tasks >> Load_Complete >> End