# Purpose: Convert semi-structured data from MongoDB into flat CSV files that will be dumped into an AWS S3 bucket to be used in Snowflake when querying in SQL

import os
import boto3
import pymongo
import csv
import io
from datetime import datetime
from bson.json_util import dumps
from collections.abc import MutableMapping

# Function to flatten nested dictionaries
def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Join lists into comma-separated string
            items.append((new_key, ', '.join(map(str, v))))
        else:
            items.append((new_key, v))
    return dict(items)

def lambda_handler(event, context):
    mongo_uri = os.environ['MONGO_URI']
    db_name = os.environ['MONGO_DB']
    bucket = os.environ['S3_BUCKET']
    
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    collections = db.list_collection_names()

    s3 = boto3.client('s3')
    
    for coll_name in collections:
        collection = db[coll_name]
        documents = list(collection.find({}))
        
        if not documents:
            continue

        flat_docs = [flatten_dict(doc) for doc in documents]
        
        headers = set()
        for doc in flat_docs:
            headers.update(doc.keys())
        headers = sorted(headers)
        
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=headers)
        writer.writeheader()
        for doc in flat_docs:
            writer.writerow({k: doc.get(k, "") for k in headers})
        
        filename = f"{coll_name}.csv"
        key = f"detail_connect/{filename}"

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )

    return {
        'statusCode': 200,
        'body': f"Exported collections from {db_name} to S3 bucket {bucket}"
    }

if __name__ == "__main__":
    event = {}
    context = None  # Mock context if needed
    response = lambda_handler(event, context)
    print(response)
