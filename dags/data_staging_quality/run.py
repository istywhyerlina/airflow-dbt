from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from pendulum import datetime
from data_staging_quality.tasks.profiling import Profiling

from helper.postgres import Execute
from airflow.models import Variable



@dag(
    dag_id = 'data_staging_quality',
    start_date = datetime(2025, 5, 16),
    schedule = None,
    catchup = False
)
def data_staging_quality():

    data_quality_create_funct = PythonOperator(
        task_id = 'data_quality_create_funct',
        python_callable = Execute._query,
        op_kwargs = {
            "connection_id": "warehouse-fp",
            "query_path": "./data_staging_quality/query/data_profile_quality_func.sql"
        }
    )
            
    create_profile_table = PythonOperator(
        task_id = 'create_profile_table',
        python_callable = Execute._query,
        op_kwargs = {
            "connection_id": "warehouse-fp",
            "query_path": "/data_staging_quality/query/create_table.sql"
        }
    )

    
    run_profiling_function = PythonOperator(
        task_id = 'run_profiling_function',
        python_callable = Profiling._src
    )

    create_profile_table >> data_quality_create_funct >> run_profiling_function 


    #extract_load()>> extract_api() >> profiling()

data_staging_quality()