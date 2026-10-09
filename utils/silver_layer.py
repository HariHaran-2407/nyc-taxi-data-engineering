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
            region_name='us-east-1',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        response = glue_client.start_job_run(
            JobName=jobName
        )

        return response['JobRunId']


