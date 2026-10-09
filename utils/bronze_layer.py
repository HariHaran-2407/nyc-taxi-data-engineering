

class BronzeLayer:

    def __init__(self):
        pass

    def ingest_data_api(self,url):
        import pandas as pd
        import requests 
        from io import BytesIO

        response = requests.get(url)
        if response.status_code== 200:
            data = response.content

            df = pd.read_parquet(BytesIO(data))

            # parquet_buffer= BytesIO()
            # df.to_parquet(parquet_buffer,index=False)

            print(df.head())

            return data
        
        else:
            print(f"Failed to fetch data. Status Code: {response.status_code}")

    
    def ingest_csv_api(self, url):
        import requests

        response = requests.get(url, timeout=60)
        response.raise_for_status()
    
        return response.content

    def put_data_s3(self,bucket_name,object_key,data):
        import os 
        from dotenv import load_dotenv
        import boto3

        load_dotenv()

        aws_access_key_id=os.getenv("aws_access_key_id")
        aws_secret_access_key=os.getenv("aws_secret_access_key")

        s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key= object_key,
            Body= data
        )

        print(f"Data uploaded into S3 Bucket '{bucket_name}' with key '{object_key}'")
        

# obj = BronzeLayer()
# url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
# filename = url.split("/")[-1]
# data = obj.ingest_data_api(url)
# obj.put_data_s3('hsariawsbucket2026',f'bronze/{filename}',data)

obj = BronzeLayer()
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
data = obj.ingest_csv_api(url)
obj.put_data_s3(
            "hariawsbucket2026",
            "bronze/lookup/taxi_zone_lookup.csv",
            data
)
