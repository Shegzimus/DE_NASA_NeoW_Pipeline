from datetime import datetime, timedelta
from airflow.decorators import dag
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
STAGING = os.environ.get("BIGQUERY_STAGING_DATASET")
WAREHOUSE = os.environ.get("BIGQUERY_WAREHOUSE_DATASET")


default_args = {
    'owner': 'Oluwasegun',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 22),
    'retries': 1,
}


@dag(
    dag_id="1_parent_etl_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags = ['HISTORICAL']
)
def parent_etl_dag():
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    trigger_test = TriggerDagRunOperator(
        task_id="trigger_test",
        trigger_dag_id="2_test_phase",
    )

    trigger_extract = TriggerDagRunOperator(
        task_id="trigger_extract",
        trigger_dag_id="3_extract_phase",
    )
    
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="4_transform_phase",
    )
    
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="5_load_phase",
    )
    
    trigger_test >> trigger_extract >> trigger_transform >> trigger_load

etl_dag = parent_etl_dag()



@dag(
    dag_id="2_test_phase",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags = ['HISTORICAL']
)

def test_dag():
    import sys
    import os
    from datetime import datetime
    from airflow.operators.python import PythonOperator

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from pipelines.extract import test_api_call, generate_time_range
    from utils.constants import nasa_api_key

    today_date = datetime.now()
    start_date, end_date, postfix = generate_time_range(today_date)

    test_api_task = PythonOperator(
    task_id="test_api_key",
    python_callable=test_api_call,
    retries=3,
    retry_delay=timedelta(minutes=1),
    op_kwargs={
        'START_DATE': start_date,
        'END_DATE': end_date,
        'API_KEY': nasa_api_key
    },
)


test = test_dag()

@dag(
    dag_id="3_extract_phase",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags = ['HISTORICAL'],
    doc_md="""
    ### Extract Phase
    This DAG handles the extraction of historical data from NASA.
    """
)
def extract_dag():
    import sys
    import os
    from airflow.operators.python import PythonOperator

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from pipelines.extract import (extract_hist_close_approach, 
                                   extract_hist_neo_data_raw, 
                                   extract_and_save_ast_data)
    
    extract_close_approach_task = PythonOperator(
        task_id="extract_close_approach",
        python_callable=extract_hist_close_approach,
        op_kwargs={"execution_date": datetime(2024, 1, 1)}
    )
    
    extract_neo_feed_task = PythonOperator(
        task_id="extract_neo_feed",
        python_callable=extract_hist_neo_data_raw,
        op_kwargs={"execution_date": datetime(2024, 1, 1)}
    )
    
    # extract_ast_data_task = PythonOperator(
    #     task_id="extract_ast_data",
    #     python_callable=extract_and_save_ast_data,
    #     op_kwargs={"start_date": datetime(2024, 1, 1)},
    # )
    extract_close_approach_task >> extract_neo_feed_task 

extract = extract_dag()



@dag(
    dag_id="4_transform_phase",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags = ['HISTORICAL'],
    doc_md="""
    ### Transform Phase
    This DAG handles the Transformation of historical data from NASA.
    """
)
def transform_dag():
    import sys
    import os
    from airflow.operators.python import PythonOperator
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from pipelines.transform import  (transform_hist_neo_feed_in_folder,
                                 transform_hist_approach_in_folder,
                                 transform_hist_asteroid_raw)
    
    transform_hist_close_approach_task = PythonOperator(
        task_id="transform_close_approach",
        python_callable=transform_hist_approach_in_folder
    )
        
    transform_hist_neo_feed_task = PythonOperator(
        task_id="transform_neo_feed",
        python_callable=transform_hist_neo_feed_in_folder
    )

    # transform_hist_ast_task = PythonOperator(
    #     task_id="transform_ast_data",
    #     python_callable=transform_hist_asteroid_raw,
    #     op_kwargs={"start_date": datetime(2020, 1, 1)},
    # )
    transform_hist_close_approach_task >> transform_hist_neo_feed_task

transform = transform_dag()


@dag(
    dag_id="5_load_phase",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags = ['HISTORICAL'],
    doc_md="""
    ### Load Phase
    This DAG handles the loading of historical data from the containers into GCS and BQ.
    """
)

def load_dag():
    import sys
    import os
    from airflow.operators.python import PythonOperator
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
    import json
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from pipelines.load import upload_folder_to_gcs

    

    folder_paths = [
    # {"local_folder": "opt/airflow/data/output/historical/asteroid_data", "gcs_prefix": "historical/asteroid_data/"},
    {"local_folder": "/opt/airflow/data/output/historical/close_approach", "gcs_prefix": "historical/close_approach/"},
    {"local_folder": "/opt/airflow/data/output/historical/neo_feed", "gcs_prefix": "historical/neo_feed/"}
    ]

    upload_to_gcs_tasks = []
    for folder in folder_paths:
        task = PythonOperator(
            task_id=f"upload_{os.path.basename(folder['local_folder'])}_to_gcs",
            python_callable=upload_folder_to_gcs,
            op_kwargs={
                "bucket_name": BUCKET,
                "local_folder": folder["local_folder"],
                "target_folder_prefix": folder["gcs_prefix"]
            },
            provide_context=True
        )
        upload_to_gcs_tasks.append(task)
    
    # load_hist_ast_to_BQ = GCSToBigQueryOperator(
    # task_id='load_hist_to_bq',
    # bucket=BUCKET,
    # source_objects='historical/asteroid_data/neo_browse_asteroid_data.parquet',
    # destination_project_dataset_table=f'{PROJECT_ID}.{STAGING}.asteroid_data',
    # source_format='parquet',
    # autodetect=True,
    # create_disposition='CREATE_IF_NEEDED',
    # write_disposition='WRITE_TRUNCATE'
    # )


    # Load local schema files
    with open('/opt/airflow/bq_schema/neo_feed_schema.json') as f:
        neo_feed_schema = json.load(f)
    with open('/opt/airflow/bq_schema/close_approach_schema.json') as f:
        close_approach_schema = json.load(f)

    stage_hist_neo_feed = BigQueryInsertJobOperator(
        task_id='stage_hist_neo_feed',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET}/historical/neo_feed/*"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": STAGING,
                    "tableId": "neo_feed"
                },
                "sourceFormat": "PARQUET",
                "timePartitioning": {
                "type": "MONTH",
                "field": "close_approach_date"
                },
                "schema": {"fields": neo_feed_schema},
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )



    
    stage_hist_approach_data = BigQueryInsertJobOperator(
        task_id='stage_hist_approach_data',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET}/historical/close_approach/*"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": STAGING,
                    "tableId": "close_approach"
                },
                "sourceFormat": "PARQUET",
                "timePartitioning": {
                "type": "MONTH",
                "field": "close_approach_date"
                },
                "schema": {"fields": close_approach_schema},
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    upload_to_gcs_tasks >> stage_hist_neo_feed >> stage_hist_approach_data

load = load_dag()


@dag(
    dag_id="6_sql_workflow",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags = ['HISTORICAL'],
    doc_md="""
    ### SQL Workflow
    This DAG handles the SQL processing of datasets from the staging layer to the warehouse layer.
    """
)

def sql_dag():
    import sys
    import os
    from airflow.operators.python import PythonOperator    

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from pipelines.sql import execute_sql_from_file

    sql_neo_feed_query = PythonOperator(
        task_id="sql_neo_feed_query",
        python_callable=execute_sql_from_file,
        op_kwargs={
            "sql_file_path": "/opt/airflow/sql/neo_feed_query.sql",
            "destination_table": f"{PROJECT_ID}.{WAREHOUSE}.neo_feed_summary"
        }
    )

