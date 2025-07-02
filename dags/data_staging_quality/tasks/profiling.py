from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.minio import MinioClient
from helper.postgres import Execute
from io import BytesIO
import json


BASE_PATH = "/opt/airflow/dags"

class Profiling:
    def _src():

        df = Execute._get_dataframe(
            connection_id = 'staging-conn',
            query_path = 'data_staging_quality/query/get_profile_quality.sql'
        )

        df['data_profile'] = df['data_profile'].apply(json.dumps)
        df['data_quality'] = df['data_quality'].apply(json.dumps)

        df.insert(
            loc = 0, 
            column = "person_in_charge", 
            value = "Isty"
        )
        df.insert(
            loc = 1, 
            column = "source", 
            value = "investment"
        )

        Execute._insert_dataframe(
                connection_id = "staging-conn", 
                query_path = "/data_staging_quality/query/insert_profile_quality.sql",
                dataframe = df
        )
