import os
import time
import sys
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))
from utils.bronze_layer import BronzeLayer
from utils.silver_layer import SilverLayer
from datetime import timedelta
from airflow.sdk import dag, task

@dag(
    schedule='@daily',
    is_paused_upon_creation=False
)

def nyz_project():

    # Task to extract data from Web API to bronze folder

    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def extract_load():

        urls = [
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet",
            "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-01.parquet",
            "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2026-01.parquet"
        ]

        obj = BronzeLayer()
        for url in urls:
            filename = url.split("/")[-1]
            table_name = filename.split("_")[0]
            data = obj.ingest_data_api(url)
            obj.put_data_s3(
                "hariawsbucket2026",
                f"bronze/{table_name}/{filename}",
                data
            )

    # Extract Lookup Data from Web API

    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def extract_load_lookup():

        url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

        obj = BronzeLayer()

        data = obj.ingest_csv_api(url)

        obj.put_data_s3(
            "hariawsbucket2026",
            "bronze/lookup/taxi_zone_lookup.csv",
            data
        )

    # Task to load data into s3 silver folder

    @task.python(retries=0)
    def transform_load_s3(ti):

        obj = SilverLayer()

        job_name = "Bronze"

        # Trigger the Glue job once
        job_run_id = obj.trigger_spark_job(job_name)
        print(f"Glue job triggered. Run ID: {job_run_id}")

        # Check the status every 30 seconds
        while True:
            status = obj.get_job_status(job_name, job_run_id)
            print(f"Glue job status: {status}")

            if status == "SUCCEEDED":
                print("Glue job completed successfully!")
                break

            elif status in ["FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"]:
                raise Exception(f"Glue job failed with status: {status}")

            else:
                print("Glue job is processing. Checking again in 30 seconds...")
                time.sleep(30)  

    # Task to trigger Glue Crawler

    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def trigger_crawler():
        obj = SilverLayer()
        crawler_name = 'crawler_silver'
        response = obj.trigger_crawler(crawler_name)
        print(response)

        
        while True:
            state, last_status = obj.get_crawler_status(crawler_name)

            print(f"Crawler state: {state}")

            if state == "READY":
                if last_status == "SUCCEEDED":
                    print("Glue Crawler completed successfully!")
                    break
                else:
                    raise Exception(
                        f"Glue Crawler failed. Last status: {last_status}"
                    )

            elif state in ["RUNNING", "STOPPING"]:
                print("Crawler is processing. Checking again in 30 seconds...")
                time.sleep(30)


    extract = extract_load()
    lookup = extract_load_lookup()
    transform = transform_load_s3()
    crawler = trigger_crawler()

    [extract, lookup] >> transform >> crawler

nyz_project_dag = nyz_project()
