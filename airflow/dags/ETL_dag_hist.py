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

from pipelines.extract import (test_api_call,
                               generate_time_range,
                               extract_hist_neo_data_raw,
                               extract_hist_close_approach,
                               extract_and_save_ast_data)

from pipelines.transform import (

)

from pipelines.load import (

)


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
    dag_id='Historic_Extract',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    max_active_runs=1,
    tags=['Historic','NASA', 'Near_Earth_Objects']
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

Task 2: Extract historical close approach data

"""
extract_hist_close_approach_task = PythonOperator(
    task_id='extract_close_approach',
    python_callable=extract_hist_close_approach("20200101"),
    dag=dag
)

"""

Task 3: Extract historical Neo data raw

"""

extract_hist_neo_data_task = PythonOperator(
    task_id='extract_neo_data_raw',
    python_callable=extract_hist_neo_data_raw("20200101"),
    dag=dag   
    )



"""
Task 4: Extract and save all Asteroid data

"""
extract_and_save_ast_data_task= PythonOperator(
    task_id='extract_and_save_ast_data',
    python_callable=extract_and_save_ast_data,
    dag=dag      
)




Begin = DummyOperator(task_id="begin", dag=dag)

Extraction_Complete = DummyOperator(task_id="Extraction_Complete", dag=dag)

Transformation_Complete = DummyOperator(task_id="Transformation_Complete", dag=dag)

Load_Complete = DummyOperator(task_id="Load_Complete", dag=dag)

End = DummyOperator(task_id="end", dag=dag)


# Set task dependencies

Begin >> test_api_task

test_api_task >> extract_hist_close_approach_task

extract_hist_close_approach_task >> extract_hist_neo_data_task >> Extraction_Complete