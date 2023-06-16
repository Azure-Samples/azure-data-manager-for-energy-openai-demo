import os
import argparse
import glob
import html
import io
import re
import time
from pypdf import PdfReader, PdfWriter
from azure.identity import AzureDeveloperCliCredential
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import *
from azure.search.documents import SearchClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from databricks.connect import DatabricksSession

MAX_SECTION_LENGTH = 1000
SENTENCE_SEARCH_LIMIT = 100
SECTION_OVERLAP = 100

parser = argparse.ArgumentParser(
    description="Prepare documents by extracting content from documents (PDF or JSON), splitting content into sections, uploading to blob storage, and indexing in a search index.",
    epilog="Example: dataprocessor.py '..\data\*' --storageaccount myaccount --container mycontainer --searchservice mysearch --index myindex -v"
    )
parser.add_argument("files", help="Files to be processed")
parser.add_argument("--category", help="Value for the category field in the search index for all sections indexed in this run")
parser.add_argument("--skipblobs", action="store_true", help="Skip uploading individual pages to Azure Blob Storage")
parser.add_argument("--storageaccount", help="Azure Blob Storage account name")
parser.add_argument("--container", help="Azure Blob Storage container name")
parser.add_argument("--storagekey", required=False, help="Optional. Use this Azure Blob Storage account key instead of the current user identity to login (use az login to set current user for Azure)")
parser.add_argument("--tenantid", required=False, help="Optional. Use this to define the Azure directory where to authenticate)")
parser.add_argument("--searchservice", help="Name of the Azure Cognitive Search service where content should be indexed (must exist already)")
parser.add_argument("--index", help="Name of the Azure Cognitive Search index where content should be indexed (will be created if it doesn't exist)")
parser.add_argument("--searchkey", required=False, help="Optional. Use this Azure Cognitive Search account key instead of the current user identity to login (use az login to set current user for Azure)")
parser.add_argument("--remove", action="store_true", help="Remove references to this document from blob storage and the search index")
parser.add_argument("--removeall", action="store_true", help="Remove all blobs from blob storage and documents from the search index")
parser.add_argument("--localpdfparser", action="store_true", help="Use PyPdf local PDF parser (supports only digital PDFs) instead of Azure Form Recognizer service to extract text, tables and layout from the documents")
parser.add_argument("--formrecognizerservice", required=False, help="Optional. Name of the Azure Form Recognizer service which will be used to extract text, tables and layout from the documents (must exist already)")
parser.add_argument("--formrecognizerkey", required=False, help="Optional. Use this Azure Form Recognizer account key instead of the current user identity to login (use az login to set current user for Azure)")
parser.add_argument("--databricksworkspaceurl", help="Azure Databricks Workspace URL, used to chunk the files and send to search index")
parser.add_argument("--databricksworkspaceid", help="Azure Databricks Workspace ID, used to chunk the files and send to search index")
parser.add_argument("--databrickskey", required=False help="Azure Databricks Access Key")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
args = parser.parse_args()

# Use the current user identity to connect to Azure services unless a key is explicitly set for any of them
azd_credential = AzureDeveloperCliCredential() if args.tenantid == None else AzureDeveloperCliCredential(tenant_id=args.tenantid, process_timeout=60)
default_creds = azd_credential if args.searchkey == None or args.storagekey == None else None
search_creds = default_creds if args.searchkey == None else AzureKeyCredential(args.searchkey)


if not args.skipblobs:
    storage_creds = default_creds if args.storagekey == None else args.storagekey
    databricks_creds = default_creds if args.databrickskey == None else args.databrickskey

databricks = DatabricksSession.builder.remote(
  host       = f"https://{args.databricksworkspaceurl}",
  token      = retrieve_token(),
  cluster_id = retrieve_cluster_id()
).getOrCreate()

def blob_name_from_file_page(filename, page = 0):
    if os.path.splitext(filename)[1].lower() == ".pdf":
        return os.path.splitext(os.path.basename(filename))[0] + f"-{page}" + ".pdf"
    else:
        return os.path.basename(filename)
    

def upload_blobs(path):
    blob_service = BlobServiceClient(account_url=f"https://{args.storageaccount}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client(args.container)
    if not blob_container.exists():
        blob_container.create_container()

    total_count = 0
    for root, dirs, files in os.walk(path):
        total_count += len(files)

    current_index = 0
    for root, dirs, files in os.walk(path):
        for file in files:
            current_index += 1
            filename = os.path.join(root, file)
            # if file is PDF split into pages and upload each page as a separate blob
            if os.path.splitext(filename)[1].lower() == ".pdf":
                reader = PdfReader(filename)
                pages = reader.pages
                for i in range(len(pages)):
                    blob_name = blob_name_from_file_page(filename, i)
                    with open(filename, "rb") as data:
                        blob_client = blob_container.get_blob_client(blob_name)
                        blob_client.upload_blob(data, max_concurrency=4, overwrite=True)
                        print(f"Uploaded file {current_index}/{total_count}: {filename} (page {i+1})")
            else:
                blob_name = blob_name_from_file_page(filename)
                with open(filename, "rb") as data:
                    blob_client = blob_container.get_blob_client(blob_name)
                    blob_client.upload_blob(data, max_concurrency=4, overwrite=True)
                    print(f"Uploaded file {current_index}/{total_count}: {filename}")

            

def remove_blobs(filename):
    if args.verbose: print(f"Removing blobs for '{filename or '<all>'}'")
    blob_service = BlobServiceClient(account_url=f"https://{args.storageaccount}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client(args.container)
    if blob_container.exists():
        if filename == None:
            blobs = blob_container.list_blob_names()
        else:
            prefix = os.path.splitext(os.path.basename(filename))[0]
            blobs = filter(lambda b: re.match(f"{prefix}-\d+\.pdf", b), blob_container.list_blob_names(name_starts_with=os.path.splitext(os.path.basename(prefix))[0]))
        for b in blobs:
            if args.verbose: print(f"\tRemoving blob {b}")
            blob_container.delete_blob(b)


if args.removeall:
    remove_blobs(None)
#    remove_from_index(None)
else:
    # if not args.remove:
    #     create_search_index()
    
    print(f"Processing files...")
    for filename in glob.glob(args.files):
        if args.verbose: print(f"Processing '{filename}'")
        if args.remove:
            remove_blobs(filename)
#            remove_from_index(filename)
        elif args.removeall:
            remove_blobs(None)
#            remove_from_index(None)
        else:
            if not args.skipblobs:
                upload_blobs(filename)
            # page_map = get_document_text(filename)
            # sections = create_sections(os.path.basename(filename), page_map)
            # index_sections(os.path.basename(filename), sections)
