import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))
from utils.bronze_layer import BronzeLayer
from datetime import timedelta
from airflow.sdk import dag, task

@dag(
    schedule='@daily',
    is_paused_upon_creation=False
)

def nyz_project():

    @task.python(retries=3,retry_delay=timedelta(seconds=5))
    def extract_load():
        # urls = [
        #     "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet",
        #     "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-01.parquet",


        # ]

        obj = BronzeLayer()
        url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
        filename = url.split("/")[-1]
        data = obj.ingest_data_api(url)
        obj.put_data_s3('hariawsbucket2026',f'bronze/{filename}',data)

    extract_load()

nyz_project_dag = nyz_project()
