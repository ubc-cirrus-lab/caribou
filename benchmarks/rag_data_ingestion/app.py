from datetime import datetime, timezone
import os
from typing import Any
import boto3
import PyPDF2
import shortuuid
import json
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.document_loaders import PyPDFLoader
from langchain_postgres.vectorstores import PGVector
from langchain.text_splitter import RecursiveCharacterTextSplitter
from sqlalchemy import create_engine

from caribou.deployment.client.caribou_workflow import CaribouWorkflow
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
EMBEDDING_MODEL_ID_DEFAULT = "amazon.titan-embed-text-v2:0"
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

# Change the following RDS and Postgres details to match your setup
bedrock_runtime_region_name = desired_region
postgresql_secret_name ="rds!db-165efb75-126f-4ae9-9540-a152c560d13f"
postgresql_host = "database-1.ct8icwysoodv.us-east-1.rds.amazonaws.com"
postgresql_dbname = "postgres"
postgresql_port = "5432"

workflow = CaribouWorkflow(name="rag_data_ingestion", version="0.0.1")

@workflow.serverless_function(
    name="upload_trigger", 
    entry_point=True,
)
def upload_trigger(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)
    
    if "user_id" in event:
        user_id: str = event["user_id"]
    else:
        raise ValueError("No user ID provided")
    if "file_name" in event:
        file_name: str = event["file_name"]
    else:
        raise ValueError("No file name provided")
    if "embedding_model_id" in event:
        embedding_model_id: str = event["embedding_model_id"]
    else:
        embedding_model_id = EMBEDDING_MODEL_ID_DEFAULT # Default Embedding Model ID

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    s3.download_file(s3_bucket_name, f"input/{file_name}", f"/tmp/{file_name}")
    with open(f"/tmp/{file_name}", "rb") as f:
        reader = PyPDF2.PdfReader(f)
        pages = str(len(reader.pages))
        filesize = str(os.path.getsize(f"/tmp/{file_name}"))

    ### Create new document & conversation history
    document, conversation = create_document_and_conversation(user_id, file_name, pages, filesize)

    document_table = boto3.resource("dynamodb", region_name=dynamodb_region_name).Table(dynamodb_document_table)
    memory_table = boto3.resource("dynamodb", region_name=dynamodb_region_name).Table(dynamodb_memory_table)

    document_table.put_item(Item=document)
    memory_table.put_item(Item=conversation)
    
    ### Create/Update ALL_DOCUMENTS document
    response = document_table.get_item(Key={"userid": user_id, "documentid": ALL_DOCUMENTS})    
    if "Item" not in response:
        documents_all, conversation_all = create_document_and_conversation(user_id, ALL_DOCUMENTS, pages, filesize)
        memory_table.put_item(Item=conversation_all)
    else:
        documents_all = response["Item"]
        documents_all["docstatus"] = UPLOADED
        documents_all["pages"] = str(int(documents_all["pages"]) + int(pages))
        documents_all["filesize"] = str(int(documents_all["filesize"]) + int(filesize))

    document_table.put_item(Item=documents_all)

    payload = {
        "document_id": document["documentid"],
        "user": user_id,
        "file_name": file_name,
        "embedding_model_id": embedding_model_id,
    }

    workflow.invoke_serverless_function(generate_embeddings, payload)

    return {"status": 200}

@workflow.serverless_function(name="generate_embeddings")
def generate_embeddings(event):
    document_id = event["document_id"]
    user_id = event["user"]
    file_name = event["file_name"]
    embedding_model_id = event["embedding_model_id"]


    document_table = boto3.resource("dynamodb", region_name=dynamodb_region_name).Table(dynamodb_document_table)

    set_doc_status(document_table, user_id, document_id, PROCESSING)
    set_doc_status(document_table, user_id, ALL_DOCUMENTS, PROCESSING)

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    s3.download_file(s3_bucket_name, f"input/{file_name}", f"/tmp/{file_name}")

    loader = PyPDFLoader(f"/tmp/{file_name}", extract_images=True)
    data = loader.load()
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=10000, chunk_overlap=1000)
    split_document = text_splitter.split_documents(data)

    bedrock_runtime = boto3.client(
        service_name="bedrock-runtime",
        region_name=bedrock_runtime_region_name,
    )

    embeddings = BedrockEmbeddings(
        model_id=embedding_model_id,
        client=bedrock_runtime,
        region_name=bedrock_runtime_region_name,
    )

    db_secret = get_db_secret()
    connection_string = f"postgresql://{db_secret['username']}:{db_secret['password']}@{postgresql_host}:{postgresql_port}/{postgresql_dbname}"
    postgresql_connection = create_engine(connection_string)
    
    collection_names = [f"{user_id}_{ALL_DOCUMENTS}", f"{user_id}_{file_name}"]
    ids = {
        f"{user_id}_{file_name}": [f"{user_id}_{file_name}_{i}" for i in range(len(split_document))],
        f"{user_id}_{ALL_DOCUMENTS}": [f"{user_id}_{file_name}_{i}_{ALL_DOCUMENTS}" for i in range(len(split_document))]
    }
    for collection_name in collection_names:
        vector_store = PGVector(
            embeddings=embeddings,
            collection_name=collection_name,
            connection=postgresql_connection,
            use_jsonb=True,
        )
    
        vector_store.add_documents(split_document, ids=ids[collection_name])

    set_doc_status(document_table, user_id, document_id, READY, ids[f"{user_id}_{file_name}"])
    set_doc_status(document_table, user_id, ALL_DOCUMENTS, READY, ids[f"{user_id}_{ALL_DOCUMENTS}"])

    # Clean up
    postgresql_connection.dispose(close=True)

    return {"status": 200}

# Helper functions for upload_trigger
def create_document_and_conversation(user_id, filename, pages, filesize):
    timestamp = datetime.now(timezone.utc)
    timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    document_id = shortuuid.uuid()
    conversation_id = shortuuid.uuid()

    document = {
        "userid": user_id,
        "documentid": ALL_DOCUMENTS if (filename == ALL_DOCUMENTS) else document_id,
        "filename": filename,
        "created": timestamp_str,
        "pages": pages,
        "filesize": filesize,
        "docstatus": UPLOADED,
        "conversations": [],
        "document_split_ids": [],
    }

    conversation = {"conversationid": conversation_id, "created": timestamp_str}
    document["conversations"].append(conversation)

    conversation = {"SessionId": conversation_id, "History": []}
    
    return [document, conversation]

# Helper functions for generate_embeddings
def set_doc_status(document_table, user_id, document_id, status, ids=None):
    if (ids):
        UpdateExpression="""
        SET docstatus = :docstatus, 
        document_split_ids = list_append(if_not_exists(document_split_ids, :empty_list), :ids)
        """
        ExpressionAttributeValues={
            ":docstatus": status,
            ":ids": ids,
            ":empty_list": []
        }
    else:
        UpdateExpression="SET docstatus = :docstatus"
        ExpressionAttributeValues={
            ":docstatus": status
        }

    document_table.update_item(
        Key={"userid": user_id, "documentid": document_id},
        UpdateExpression=UpdateExpression,
        ExpressionAttributeValues=ExpressionAttributeValues,
    )

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