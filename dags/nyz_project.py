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
        bucket="harinyzbucket2026"
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
                "harinyzbucket2026",
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
            "harinyzbucket2026",
            "bronze/lookup/taxi_zone_lookup.csv",
            data
        )

    # Task to load data into s3 silver folder

    @task.python(retries=0)
    def silver_transform(ti):

        obj = SilverLayer()

        job_name = "nyz_bronze_to_silver"

        # Trigger the Glue job once
        job_run_id = obj.trigger_spark_job(job_name)
        print(f"Glue job triggered. Run ID: {job_run_id}")

        # Check the status every 30 seconds
        while True:
            status = obj.get_job_status(job_name, job_run_id)

            if status == "SUCCEEDED":
                print("Glue job completed successfully!")
                break

            elif status in ["FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"]:
                raise Exception(f"Glue job failed with status: {status}")

            else:
                time.sleep(10)  

    # Task to trigger Glue Crawler

    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def silver_crawler():
        obj = SilverLayer()
        crawler_name = 'nyz_crawler'
        response = obj.trigger_crawler(crawler_name)
        print(response)

        while True:
            state, last_status = obj.get_crawler_status(crawler_name)

            if state == "READY":
                if last_status == "SUCCEEDED":
                    print("Glue Crawler completed successfully!")
                    break
                else:
                    raise Exception(
                        f"Glue Crawler failed. Last status: {last_status}"
                    )

            elif state in ["RUNNING", "STOPPING"]:
                time.sleep(10)


    # Task to run Gold Fact Taxi Trip Glue job
    @task.python(retries=0)
    def gold_transform():
        obj = SilverLayer()
        job_name = "nyz_gold_layer"

        job_run_id = obj.trigger_spark_job(job_name)
        print(f"Gold Glue job triggered. Run ID: {job_run_id}")

        while True:
            status = obj.get_job_status(job_name, job_run_id)

            if status == "SUCCEEDED":
                print("Gold Glue job completed successfully!")
                break

            elif status in [
                "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"
            ]:
                raise Exception(
                    f"Gold Glue job failed with status: {status}"
                )

            else:
                time.sleep(10)

    # Task to trigger Gold Crawler
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def gold_crawler():
        obj = SilverLayer()

        crawler_name = "nyz_gold_crawler"

        obj.trigger_crawler(crawler_name)
        print(f"Gold crawler triggered: {crawler_name}")

        while True:
            state, last_status = obj.get_crawler_status(crawler_name)
           
            if state == "READY":
                if last_status == "SUCCEEDED":
                    print("Gold crawler completed successfully!")
                    break
                else:
                    raise Exception(
                        f"Gold crawler failed. Last status: {last_status}"
                    )

            elif state in ["RUNNING", "STOPPING"]:
                time.sleep(10)
    
    extract = extract_load()
    lookup = extract_load_lookup()
    transform = silver_transform()
    crawler = silver_crawler()

    gold_fact = gold_transform()
    gold_catalog = gold_crawler()

    [extract, lookup] >> transform >> crawler
    crawler >> gold_fact >> gold_catalog

nyz_project_dag = nyz_project()
