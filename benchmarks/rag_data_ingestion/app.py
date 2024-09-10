from datetime import datetime, timezone
import os
from typing import Any
from urllib.parse import quote_plus
import boto3
import PyPDF2
import shortuuid
import urllib
import json
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.document_loaders import PyPDFLoader
from langchain_postgres.vectorstores import PGVector
from langchain.text_splitter import RecursiveCharacterTextSplitter
from sqlalchemy import create_engine
import psycopg2

import faiss
import numpy as np
from caribou.deployment.client.caribou_workflow import CaribouWorkflow
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
EMBEDDING_MODEL_ID_DEFAULT = "amazon.titan-embed-text-v2"
UPLOADED = "UPLOADED"
ALL_DOCUMENTS = "ALL_DOCUMENTS"
PROCESSING = "PROCESSING"
READY = "READY"

# Change the following bucket and dynamodb name and region to match your setup
desired_region = "us-east-1"
secrets_manager_region = desired_region

s3_bucket_name = "caribou-document-embedding-benchmark"
s3_bucket_region_name = desired_region
dynamodb_document_table = "caribou-document-embedding-benchmark-document"
dynamodb_memory_table = "caribou-document-embedding-benchmark-memory"
dynamodb_region_name = s3_bucket_region_name

bedrock_runtime_region_name = desired_region
# postgresql_secret_name = "caribou-document-embedding-benchmark-postgresql"
postgresql_secret_name ="rds!db-165efb75-126f-4ae9-9540-a152c560d13f"
postgresql_host = "database-1.ct8icwysoodv.us-east-1.rds.amazonaws.com"
postgresql_dbname = "database-1"
# workflow = CaribouWorkflow(name="rag_data_ingestion", version="0.0.1")

# Change the following bucket name and region to match your setup
s3_bucket_name = "caribou-dna-visualization"
s3_bucket_region_name = "us-east-1"

workflow = CaribouWorkflow(name="rag_data_ingestion", version="0.0.1")

@workflow.serverless_function(
    name="upload_trigger", 
    entry_point=True,
)
def upload_trigger(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)
    
    # Example usage:
    db_secret = get_db_secret()
    username = quote_plus(db_secret['username'])
    password = quote_plus(db_secret['password'])
    connection_str = f"postgresql+psycopg2://{username}:{password}@{postgresql_host}:5432/{postgresql_dbname}?sslmode=require"

    if test_server_availability(connection_str):
        print("Server is available!")
        logger.info("Server is available!")
    else:
        print("Server is unavailable.")
        logger.error("Server is unavailable.")

def test_server_availability(connection_str):
    # try:
    #     # Attempt to create a connection to the database using SQLAlchemy
    #     engine = create_engine(connection_str)
    #     with engine.connect() as connection:
    #         # Run a simple query to test the connection
    #         connection.execute("SELECT 1")
    #     return True
    # except Exception as e:
    #     print(f"Failed to connect to the database: {e}")
    #     return False

    # Define your connection details
    host = postgresql_host
    port = "5432"
    dbname = postgresql_dbname
    user = db_secret['username']
    password = db_secret['password']

    db_secret = get_db_secret()

    # Create a connection to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        print("Connection established")

        # Create a cursor object
        cursor = conn.cursor()

        # Example: Execute a query
        cursor.execute("SELECT version();")
        
        # Fetch and print result
        db_version = cursor.fetchone()
        print(f"Database version: {db_version}")
        
        # Close the cursor and connection
        cursor.close()
        conn.close()

        return True

    except Exception as e:
        return False

def get_db_secret():
    sm_client = boto3.client(
        service_name="secretsmanager",
        region_name=secrets_manager_region,
    )
    response = sm_client.get_secret_value(
        SecretId=postgresql_secret_name
    )["SecretString"]
    secret = json.loads(response)
    return secret
