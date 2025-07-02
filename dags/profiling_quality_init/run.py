from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from pendulum import datetime
from helper.minio import MinioClient
from helper.postgres import Execute

@dag(
    dag_id = 'profiling_quality_init',
    start_date = datetime(2025, 5, 16),
    schedule = "@once",
    catchup = False
)

def profiling_quality_init():
    data_quality_create_funct = PythonOperator(
        task_id = 'data_quality_create_funct',
        python_callable = Execute._query,
        op_kwargs = {
            "connection_id": "staging-conn",
            "query_path": "./profiling_quality_init/query/data_profile_quality_func.sql"
        }
    )
            
    create_profile_table = PythonOperator(
        task_id = 'create_profile_table',
        python_callable = Execute._query,
        op_kwargs = {
            "connection_id": "staging-conn",
            "query_path": "/profiling_quality_init/query/create_table.sql"
        }
    )

    create_profile_table >> data_quality_create_funct 

profiling_quality_init()