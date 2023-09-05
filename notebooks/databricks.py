# Databricks notebook source
# Install required modules
%pip install azure-core==1.27.1
%pip install azure-search-documents==11.4.0b3
%pip install azure-storage-blob==12.14.1
%pip install azure-identity==1.13.0b4

# COMMAND ----------
# Import required modules
import os
from typing import Dict
import base64
import argparse
import glob
import html
import io
import re
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes.models import (
    ComplexField,
    CorsOptions,
    SearchIndex,
    ScoringProfile,
    SearchFieldDataType,
    SimpleField,
    SearchableField
)
import json
from io import BytesIO
import concurrent.futures
import zipfile
from azure.storage.blob import BlobServiceClient
from multiprocessing import Pool
from azure.identity import DefaultAzureCredential

# COMMAND ----------
# Define connection info

# Get the service endpoint and API key from the environment
endpoint = dbutils.widgets.get("endpoint")
index_name = dbutils.widgets.get("index_name")
connection_string = dbutils.widgets.get("connection_string")
container_name = dbutils.widgets.get("container_name")

print(endpoint)

default_creds = DefaultAzureCredential()

# Create a service client
search_client = SearchClient(endpoint, index_name, credential=default_creds)

# Create a BlobServiceClient object using the connection string
blob_service_client = BlobServiceClient(account_url=connection_string, credential=default_creds)

# COMMAND ----------
# Stage zipped data

# Verify that ZIP file exist
blob_name = "TNO.zip"
source_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name )
# Check if the blob exists
blob_exists = source_blob_client.exists()

if blob_exists:
    print("Blob exists.")
else:
    print("Did not find TNO.zip")


# Download the source blob into memory
downloaded_blob = source_blob_client.download_blob()
zip_data = downloaded_blob.content_as_bytes()

# Function to extract data
def extract_and_upload(file_info):
    extracted_data = zip_ref.read(file_info.filename)
    destination_blob_name = file_info.filename
    destination_blob_client = blob_service_client.get_blob_client(container=container_name, blob=destination_blob_name)
    destination_blob_client.upload_blob(extracted_data, overwrite=True)

# Extract the zip file in memory
with BytesIO(zip_data) as zip_stream:
    with zipfile.ZipFile(zip_stream, 'r') as zip_ref:
        # Create a ThreadPoolExecutor with a maximum of 10 worker threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Iterate through each file in the zip and submit tasks to the executor
            futures = [executor.submit(extract_and_upload, file_info) for file_info in zip_ref.infolist()]
            
            # Wait for all tasks to complete
            concurrent.futures.wait(futures)

#Delete the zipfile
blob_to_delete = "TNO.zip"
source_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_to_delete)
source_blob_client.delete_blob()
# Define functions

# Split JSON into smaller chunks
def split_json(data, chunk_size):
    chunks = []
    start_idx = 0
    array_level = 0

    while start_idx < len(data):
        # Find the end index of the next JSON object
        end_idx = start_idx + chunk_size
        while end_idx < len(data):
            if data[end_idx] == '[':
                array_level += 1
            elif data[end_idx] == ']':
                array_level -= 1

            if array_level == 0 and data[end_idx] == ',':
                break

            end_idx += 1

        # Extract the JSON object and add it to the chunks
        chunk = data[start_idx:end_idx]
        chunks.append(chunk)

        start_idx = end_idx + 1

    return chunks

def extract_id_and_content(blob_content):
    data = json.loads(blob_content)
    id = data.get("id")
    content = json.dumps(data).replace('"', "'")
    kind = data.get("kind")
    return id, content, kind


def extract_id(blob_content):
    data = json.loads(blob_content)
    id = data.get("id")
    return id


def encode_id(id_value):
    return id_value.replace(":", "_")


def process_blob(blob_name):
    # Get the blob client for the current blob
    blob_client = container_client.get_blob_client(blob_name)
    # Download the blob's content
    blob_content = blob_client.download_blob().readall()
    blob_name = blob_client.blob_name
    id, content, kind = extract_id_and_content(blob_content)
    id_encoded = base64.urlsafe_b64encode(id.encode()).decode()
    category_split = kind.split(':')
    category_split2 = category_split[-2].split('--')
    category = category_split2[-1]
    #print('id: ' + id)
    output_fields = []
    result_split = split_json(json.dumps(json.loads(blob_content)), chunk_size)
    for i, chunk in enumerate(result_split):
        document = {
            "id": str(id),
            "content": "ID " + str(id) + ", " + chunk,
            "kind": kind,
            "keyfield": encode_id(id_encoded)+"-"+str(i),
            "sourcefile": blob_name,
            "category": category,
            "sourcepage": blob_name + "-" + str(i)
        }
        output_fields.append(document)
    result = search_client.upload_documents(documents=output_fields)

# COMMAND ----------
# Execute populate index
if __name__ == '__main__':
    # Example usage
    chunk_size = 800
   
    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs (documents) in the container
    blob_list = container_client.list_blobs()
    counter = 0

    with Pool() as pool:
        pool.map(process_blob, [blob.name for blob in blob_list])

    exit()