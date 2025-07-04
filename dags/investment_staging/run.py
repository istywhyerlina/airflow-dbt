from airflow.decorators import dag
from pendulum import datetime
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.decorators import task_group
from investment_staging.tasks.main import extract_db, extract_api, load_db, load_api
from helper.callbacks.slack_notifier import slack_notifier
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# default_args = {
#     'on_failure_callback': slack_notifier
# }
default_args = {
    'on_failure_callback': send_slack_notification(
        slack_conn_id="slack_conn",
        channel="airflow-notifications",
        text="There is an ERROR")}

@dag(
    dag_id='investment_staging',
    description='Extract data and load into staging area',
    start_date=datetime(2025, 1, 7),
    schedule="@once",
    default_args=default_args
)

def bikestore_staging():
    incremental_mode = eval(Variable.get('INVESTMENT_INCREMENTAL_MODE'))
    @task_group
    def extract(incremental):
        extract_db(incremental=incremental) 
        extract_api()

    @task_group
    def load_stg(incremental):
        load_db(incremental=incremental) 
        load_api()        
    @task_group
    def trigger_DAGs():
        trigger_profiling= TriggerDagRunOperator(task_id='trigger_profiling',trigger_dag_id='data_staging_quality',wait_for_completion=True,trigger_rule='none_failed')
        trigger_warehouse= TriggerDagRunOperator(task_id='trigger_warehouse',trigger_dag_id='investment_warehouse',wait_for_completion=True,trigger_rule='none_failed')
        trigger_profiling
        trigger_warehouse
    #extract(incremental=incremental_mode)
    extract(incremental=incremental_mode) >> load_stg(incremental=incremental_mode) >> trigger_DAGs()
    #load_stg(incremental=incremental_mode) >> trigger_warehouse

bikestore_staging()