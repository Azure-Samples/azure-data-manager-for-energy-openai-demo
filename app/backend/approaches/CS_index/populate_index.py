import os
from dotenv import load_dotenv
from typing import Dict
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
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

load_dotenv()

# Get the service endpoint and API key from the environment
endpoint = os.environ["COGNITIVE_SEARCH_INSTANCE_URL"]
key = os.environ["COGNITIVE_SEARCH_KEY"]
index_name = os.environ["COGNITIVE_SEARCH_INDEX_NAME"]

## Create a service client
search_client = SearchClient(endpoint, index_name, AzureKeyCredential(key))

import json

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


def extract_id(blob_content):
    data = json.loads(blob_content)
    id = data.get("id")
    return id

def encode_id(id_value):
    return id_value.replace(":", "_")

# Example usage
chunk_size = 800

# Convert the JSON string to a Python object
from azure.storage.blob import BlobServiceClient
#if needed, run: pip install azure-storage-blob

# Define your Azure Storage connection string
connction_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

# Create a BlobServiceClient object using the connection string
blob_service_client = BlobServiceClient.from_connection_string(connction_string)

# Define the name of your container
container_name = os.environ["AZURE_STORAGE_NAME"]

# Get a reference to the container
container_client = blob_service_client.get_container_client(container_name)

# List all blobs (documents) in the container
blob_list = container_client.list_blobs()
counter = 0

# Iterate over the blobs and read their contents
for blob in blob_list:
    # Get the blob client for the current blob
    blob_client = container_client.get_blob_client(blob.name)
    
    # Download the blob's content
    blob_content = blob_client.download_blob().readall()
    blob_name = blob_client.blob_name
    id = extract_id(blob_content)
    print('id: ' +id)
    output_fields = []
    result_split = split_json(json.dumps(json.loads(blob_content)), chunk_size)
    for i, chunk in enumerate(result_split):        
        document = {
            "id": str(id),
            "content": chunk.replace('"', "'"),
            "keyfield": encode_id(id)+"-"+str(i),
            "sourcefile": blob_name,
            "category": "",
            "sourcepage": blob_name + "-"+ str(i)
        }
        output_fields.append(document)
        result = search_client.upload_documents(documents=output_fields)
        counter += 1
        print("Upload of new document succeeded: {}".format(result[0].succeeded)+ ' counter = '+ str(counter))
        #if counter > 2:
        #    exit()
        print(f"Chunk {i+1}: {chunk}\n")

    
  

