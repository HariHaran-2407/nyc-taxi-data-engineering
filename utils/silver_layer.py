import boto3
import os
from dotenv import load_dotenv
load_dotenv()

class SilverLayer():

    def __init__(self):
        pass

    def trigger_spark_job(self,jobName):

        aws_access_key_id=os.getenv("aws_access_key_id")
        aws_secret_access_key=os.getenv("aws_secret_access_key")

        glue_client = boto3.client(
            'glue',
            region_name='ap-south-2',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        response = glue_client.start_job_run(
            JobName=jobName
        )

        return response['JobRunId']

    def get_job_status(self, jobName, jobRunId):
        glue_client = boto3.client(
            "glue",
            region_name="ap-south-2",
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key")
        )

        response = glue_client.get_job_run(
            JobName=jobName,
            RunId=jobRunId
        )
        return response["JobRun"]["JobRunState"]

    def trigger_crawler(self,crawler_name):
        glue_client = boto3.client(
            "glue",
            region_name="ap-south-2",
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key")
        )

        response = glue_client.start_crawler(
             Name=crawler_name
        )

        return response

    
    def get_crawler_status(self, crawler_name):
        glue_client = boto3.client(
            "glue",
            region_name="ap-south-2",
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key")
        )

        response = glue_client.get_crawler(Name=crawler_name)

        state = response["Crawler"]["State"]
        last_status = response["Crawler"].get("LastCrawl", {}).get("Status")

        return state, last_status

if __name__ == "__main__":
    
    obj = SilverLayer()
    response = obj.trigger_crawler('crawler_silver')
    print(response)

