from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from pendulum import datetime
from data_staging_quality.tasks.profiling import Profiling
from data_staging_quality.tasks.extract import Extract
from data_staging_quality.tasks.load import Load

from helper.postgres import Execute
from airflow.models import Variable


tables_to_extract=eval(Variable.get('tables_to_extract'))
incremental = eval(Variable.get('incremental'))

@dag(
    dag_id = 'data_staging_quality',
    start_date = datetime(2025, 5, 16),
    schedule = "@daily",
    catchup = False
)
def data_staging_quality():
    @task_group
    def extract_src():
        @task_group
        def extract_load():
            previous_task = None
            for table_name in tables_to_extract:
                current_task = PythonOperator(
                    task_id = f'extract_{table_name}',
                    python_callable = Extract._src_db,
                    op_kwargs={'table_name': table_name, 'incremental': incremental}
                    )
                if previous_task:
                        previous_task >> current_task
            
                previous_task = current_task

        @task_group
        def extract_api():
            extract = PythonOperator(
                task_id = 'extract',
                python_callable = Extract._src_api,
                trigger_rule = 'none_failed',

            )

            load = PythonOperator(
                task_id = 'load',
                python_callable = Load._src_api,
                trigger_rule = 'none_failed'
            )

            extract >> load
        
        extract_load()
        extract_api()

    @task_group
    def profiling():
        data_quality_create_funct = PythonOperator(
            task_id = 'data_quality_create_funct',
            python_callable = Execute._query,
            op_kwargs = {
                "connection_id": "staging-conn",
                "query_path": "./data_staging_quality/query/data_profile_quality_func.sql"
            }
        )
                
        create_profile_table = PythonOperator(
            task_id = 'create_profile_table',
            python_callable = Execute._query,
            op_kwargs = {
                "connection_id": "staging-conn",
                "query_path": "/data_staging_quality/query/create_table.sql"
            }
        )

        
        run_profiling_function = PythonOperator(
            task_id = 'run_profiling_function',
            python_callable = Profiling._src
        )

        create_profile_table >> data_quality_create_funct >> run_profiling_function 
    
    extract_src()>> profiling()
    #extract_load()>> extract_api() >> profiling()

data_staging_quality()