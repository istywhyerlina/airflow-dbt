from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow import AirflowException
from helper.minio import MinioClient
from helper.minio import CustomMinio
from datetime import timedelta
from airflow.models import Variable
from pangres import upsert
from sqlalchemy import create_engine
import json



import pandas as pd
import requests

BASE_PATH = "/opt/airflow/dags"


class Extract:
    @staticmethod
    def _src_db(table_name, incremental, **kwargs):
        """
        Extract data from source database.

        Args:
            table_name (str): Name of the table to extract data from.
            incremental (bool): Whether to extract incremental data or not.
            **kwargs: Additional keyword arguments.

        Raises:
            AirflowException: If failed to extract data from source database.
            AirflowSkipException: If no new data is found.
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id='source-conn')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            query = f"SELECT * FROM public.{table_name}"
            if incremental:
                date = kwargs['ds']
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

                object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            
            else:
                object_name = f'/temp/{table_name}.csv'

            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            connection.commit()
            connection.close()

            column_list = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_list)

            if incremental:
                if df.empty:
                    kwargs['ti'].xcom_push(key=object_name, value={'data':table_name,'status': "skipped" })
                    raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")
                else:
                    kwargs['ti'].xcom_push(key=object_name, value={'data':table_name,'status': "success" })
            else:
                kwargs['ti'].xcom_push(key=object_name, value={'data':table_name,'status': "success" })
            
            table_pkey = eval(Variable.get('tables_to_load'))
            df = df.set_index(table_pkey[table_name])
            print(df.head())
            engine_stg = create_engine(PostgresHook(postgres_conn_id='staging-conn').get_uri())

            upsert(
                con=engine_stg,
                df=df,
                table_name=table_name,
                schema='public',
                if_row_exists='update'
            )

        except AirflowSkipException as e:
            raise e
        except Exception as e:
            raise AirflowException(f"Error when extracting {table_name} : {str(e)}")


    @staticmethod
    def _src_api(ds):
        """
        Extract data from data API.

        Args:
            ds (str): Date string.

        Raises:
            AirflowException: If failed to fetch data from data API.
            AirflowSkipException: If no new data is found.
        """
        try:

            start_date='2008-06-09'
            end_date='2011-06-16'
            
            url_api=Variable.get('src_api_url')
            print(f"--------------URL API: {url_api}")
            response = requests.get(
                    url=f'{url_api}start_date={start_date}&end_date={end_date}',
                )
            

            if response.status_code != 200:
                raise AirflowException(f"Failed to fetch data from data API. Status code: {response.status_code}")

            json_data = response.json()
            if not json_data:
                raise AirflowSkipException("No new data in data API. Skipped...")
            
            bucket_name = 'extracted-data'
            object_name = f'/temp/data_api_{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'
            CustomMinio._put_json(json_data, bucket_name, object_name)
           
        except AirflowSkipException as e:
            raise e
        
      
        except Exception as e:
            raise AirflowException(f"Error when extracting data API: {str(e)}")