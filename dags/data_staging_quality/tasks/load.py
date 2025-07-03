from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException,AirflowException
from airflow import AirflowException
import pandas as pd
from helper.minio import MinioClient, CustomMinio
from sqlalchemy import create_engine
from pangres import upsert
from datetime import timedelta
import json

class Load:
    @staticmethod
    def _src_api(ds):
        """
        Load data from Data API into staging area.

        Args:
            ds (str): Date string for the data to load.
        """
        bucket_name = 'extracted-data'
        object_name = f'/temp/data_api_{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'

        try:
            engine_stg = create_engine(PostgresHook(postgres_conn_id='staging-conn').get_uri())

            try:
                minio_client = MinioClient._get()

                try:
                    data = minio_client.get_object(bucket_name=bucket_name, object_name=object_name).read().decode('utf-8')
                except:
                    raise AirflowSkipException(f"src_api doesn't have new data. Skipped...")
                
                data = json.loads(data)
                df = pd.json_normalize(data)
                df = df.set_index('milestone_id')

                upsert(
                    con=engine_stg,
                    df=df,
                    table_name='milestones',
                    schema='public',
                    if_row_exists='update'
                )
            except AirflowSkipException as e:
                engine_stg.dispose()
                raise e
            
            except Exception as e:
                engine_stg.dispose()
                raise AirflowException(f"Error when loading data from Data API: {str(e)}")
            
        except AirflowSkipException as e:
            raise e
            
        except Exception as e:
            raise e
