from airflow.decorators import task,task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from investment_staging.tasks.components.extract import Extract
from investment_staging.tasks.components.load import Load
from airflow.datasets import Dataset

@task_group
def extract_db(incremental):            
    table_pkey = eval(Variable.get('INVESTMENT__table_to_extract_and_load'))
    table_to_extract = list(table_pkey.keys())

    for table_name in table_to_extract:
        current_task = PythonOperator(
            task_id = f'{table_name}',
            python_callable = Extract._src_db,
            trigger_rule = 'none_failed',
            outlets = [Dataset(f'postgres://sourcesfp:5432/postgres.{table_name}')],
            op_kwargs = {
                'table_name': f'{table_name}',
                'incremental': incremental,
                'table_pkey': table_pkey,
            }
        )

        current_task
        
@task_group
def extract_api():
    extract = PythonOperator(
        task_id = 'extract_api',
        python_callable = Extract._src_api,
        trigger_rule = 'none_failed',
        outlets = [Dataset(f'https://api-milestones.vercel.app/api/data?start_date=2008-06-09&end_date=2011-06-16')]
    )

@task_group
def extract_minio():            
    table_pkey = eval(Variable.get('MINIO__table_to_extract_and_load'))
    table_to_extract = list(table_pkey.keys())

    for table_name in table_to_extract:
        current_task = PythonOperator(
            task_id = f'{table_name}',
            python_callable = Extract._src_minio,
            trigger_rule = 'none_failed',
            outlets = [Dataset(f's3://api-pipeline.minio-devops-class.pacmann.ai/file-source/{table_name}')],
            op_kwargs = {
                'table_name': f'{table_name}'
            }
        )

        current_task

@task_group
def extract_csv():            
    table_pkey = eval(Variable.get('MINIO__table_to_extract_and_load'))
    table_to_extract = list(table_pkey.keys())

    for table_name in table_to_extract:
        current_task = PythonOperator(
            task_id = f'{table_name}',
            python_callable = Extract._src_csv,
            trigger_rule = 'none_failed',
            outlets = [Dataset(f'src_csv/{table_name}')],
            op_kwargs = {
                'table_name': f'{table_name}'
            }
        )

        current_task

@task_group
def load_csv():            
    table_pkey = eval(Variable.get('MINIO__table_to_extract_and_load'))
    table_to_load = list(table_pkey.keys())

    for table_name in table_to_load:
        current_task = PythonOperator(
            task_id = f'{table_name}',
            python_callable = Load._minio,
            trigger_rule = 'none_failed',
            outlets = [Dataset(f'postgres://warehouse-fp:5432/postgres.staging.{table_name}')],
            op_kwargs = {
                'table_name': f'{table_name}',
                'table_pkey': table_pkey,
            }
        )

        current_task


@task_group
def load_db(incremental):
    table_pkey = eval(Variable.get('INVESTMENT__table_to_extract_and_load'))
    table_to_load = list(table_pkey.keys())
    previous_task = None
    
    for table_name in table_to_load:
        current_task = PythonOperator(
            task_id = f'{table_name}',
            python_callable = Load._investment,
            trigger_rule = 'none_failed',
            outlets = [Dataset(f'postgres://warehouse-fp:5432/postgres.staging.{table_name}')],
            op_kwargs = {
                'table_name': table_name,
                'table_pkey': table_pkey,
                'incremental': incremental
            },
        )

        if previous_task:
            previous_task >> current_task

        previous_task = current_task

@task_group
def load_api():
        table_name ='milestone'
        table_pkey='milestone_id'
        api = PythonOperator(
            task_id = f'{table_name}',
            python_callable = Load._api,
            trigger_rule = 'none_failed',
            outlets = [Dataset(f'postgres://warehouse-fp:5432/postgres.staging.{table_name}')],
            op_kwargs = {
                'table_name': table_name,
                'table_pkey': table_pkey,
            },
        )

