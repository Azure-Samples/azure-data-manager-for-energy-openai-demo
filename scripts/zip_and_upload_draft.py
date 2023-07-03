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

##Local fun
import zipfile
do_zip = True
# Directory to be zipped
directory_to_zip = '../TNO/'

# Zip file name and path
zip_file_name = 'TNO_archive2.zip'
zip_file_path = '../zipped/' + zip_file_name

# Zip the directory
if do_zip : 
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(directory_to_zip):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, directory_to_zip))

with open(zip_file_path, 'rb') as file: container_client.upload_blob(name=zip_file_name, data=file)


blob_name = "TNO_archive.zip"
blob_client = blob_service_client.get_blob_client(container=container_name, blob=b  )
# Check if the blob exists
blob_exists = blob_client.exists()

if blob_exists:
    print("Blob exists.")