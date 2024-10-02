import requests
import logging
import pymongo
from clickhouse_driver import Client
from datetime import datetime
import time
import sys
# Constants
GROUP_IB_API_URL = "https://api.group-ib.com/endpoint"  # Replace with the actual endpoint
API_KEY = "your_group_ib_api_key"
MONGODB_URI = "mongodb://localhost:27017"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_DB = "TpXuJW6M6rgdkF3ra9OQ"
CLICKHOUSE_TABLE = "your_table"
# MongoDB Setup
mongo_client = pymongo.MongoClient(MONGODB_URI)
mongo_db = mongo_client["logs_db"]
mongo_collection = mongo_db["api_logs"]
# ClickHouse Setup
clickhouse_client = Client(CLICKHOUSE_HOST, database=CLICKHOUSE_DB)
# Logger Setup
logger = logging.getLogger('pipeline_logger')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)  # Log to console
file_handler = logging.FileHandler('pipeline.log')  # Log to a file (optional)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)
def log_to_mongo(data):
   """ Log structured data to MongoDB """
   try:
       mongo_collection.insert_one(data)
   except Exception as e:
       logger.error(f"Failed to log to MongoDB: {str(e)}")
def fetch_data_from_group_ib():
   """ Fetch data from Group IB API using REST """
   headers = {
       "Authorization": f"Bearer {API_KEY}",
       "Content-Type": "application/json"
   }
   for _ in range(3):  # Retry mechanism
       try:
           response = requests.get(GROUP_IB_API_URL, headers=headers)
           response.raise_for_status()
           return response.json()
       except requests.exceptions.RequestException as e:
           log_data = {
               "timestamp": datetime.now(),
               "level": "ERROR",
               "message": f"Failed to fetch data from Group IB API: {str(e)}"
           }
           logger.error(log_data["message"])
           log_to_mongo(log_data)
           time.sleep(5)  # Wait before retry
   return None
def transform_data(api_data):
   """ Transform API response to match ClickHouse schema """
   transformed = []
   for record in api_data.get("data", []):
       transformed_record = {
           "id": record.get("id"),
           "field1": record.get("field1"),
           "field2": record.get("field2"),
           # Add more fields as per your schema
           "fetched_at": datetime.now()
       }
       transformed.append(transformed_record)
   return transformed
def insert_data_into_clickhouse(data):
   """ Insert transformed data into ClickHouse """
   if not data:
       return
   query = f"INSERT INTO {CLICKHOUSE_TABLE} (id, field1, field2, fetched_at) VALUES"
   values = [(d['id'], d['field1'], d['field2'], d['fetched_at']) for d in data]
   try:
       clickhouse_client.execute(query, values)
       log_data = {
           "timestamp": datetime.now(),
           "level": "INFO",
           "message": f"Inserted {len(values)} records into ClickHouse"
       }
       logger.info(log_data["message"])
       log_to_mongo(log_data)
   except Exception as e:
       log_data = {
           "timestamp": datetime.now(),
           "level": "ERROR",
           "message": f"Failed to insert data into ClickHouse: {str(e)}"
       }
       logger.error(log_data["message"])
       log_to_mongo(log_data)
def run_pipeline():
   """ Main pipeline execution - continuous loop """
   while True:
       api_data = fetch_data_from_group_ib()
       if api_data:
           transformed_data = transform_data(api_data)
           insert_data_into_clickhouse(transformed_data)
       time.sleep(60)  # Poll every 60 seconds
if __name__ == "__main__":
   run_pipeline()