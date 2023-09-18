import os
import argparse
import glob
import re
import base64
from typing import Dict
from azure.identity import AzureDeveloperCliCredential, AzureCliCredential
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import *
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service.jobs import JobTaskSettings, NotebookTask, NotebookTaskSource



# MAX_SECTION_LENGTH = 1000
# SENTENCE_SEARCH_LIMIT = 100
# SECTION_OVERLAP = 100

parser = argparse.ArgumentParser(
    description="Prepare documents by extracting content from documents (PDF or JSON), splitting content into sections, uploading to blob storage, and indexing in a search index.",
    epilog="Example: dataprocessor.py '..\data\*' --storageaccount myaccount --container mycontainer --searchservice mysearch --index myindex -v"
)
parser.add_argument("files", help="Files to be processed")
parser.add_argument("--category", help="Value for the category field in the search index for all sections indexed in this run")
parser.add_argument("--skipblobs", action="store_true", help="Skip uploading data to Azure Blob Storage")
parser.add_argument("--skipindex", action="store_true", help="Optional. Skip populating the index using Databricks")
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
parser.add_argument("--databrickskey", required=False, help="Azure Databricks Access Key")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
args = parser.parse_args()

# Use the current user identity to connect to Azure services unless a key is explicitly set for any of them
azd_credential = AzureDeveloperCliCredential() if args.tenantid == None else AzureDeveloperCliCredential(
    tenant_id=args.tenantid, process_timeout=60)
az_credential = AzureCliCredential() if args.tenantid == None else AzureCliCredential(
    tenant_id=args.tenantid, process_timeout=60)
default_creds = azd_credential if args.searchkey == None or args.storagekey == None else None
default_az_creds = az_credential if args.databrickskey == None else None
search_creds = default_creds if args.searchkey == None else AzureKeyCredential(
    args.searchkey)

if (not args.skipindex == True):
    databricks_creds = default_az_creds.get_token(
        '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d') if args.databrickskey == None else args.databrickskey
if (not args.skipblobs == True):
    storage_creds = default_creds if args.storagekey == None else args.storagekey


def populate_index_with_databricks():
    w = WorkspaceClient(
        host=args.databricksworkspaceurl,
        token=databricks_creds.token
    )
    clusterid = None  # Initialize clusterid variable

    
    clusters = w.clusters.list()

    if not clusters:
        print('Creating databricks cluster')

        c = w.clusters.create(
            cluster_name=args.databricksworkspaceid,
            spark_version='12.2.x-scala2.12',
            node_type_id='Standard_D8ads_v5',
            autotermination_minutes=30,
            num_workers=2
        )

        print(f"The databricks cluster is now ready at "
              f"{w.config.host}#setting/clusters/{c.cluster_id}/configuration\n")

        clusterid = c.cluster_id
    else:
        cluster = clusters[0]  # Assuming you only want to use the first cluster in the list
        print(f"Reusing existing Databricks cluster at "
              f"{w.config.host}#setting/clusters/{cluster.cluster_id}/configuration\n")
        clusterid = cluster.cluster_id

    print('Uploading jupyter notebook')
    notebook_path = '/create_cs_index'
    
    
    databricks_script = open(f"./notebooks/databricks.py", "r")
    content = databricks_script.read()
    databricks_script.close()

    w.workspace.import_(path=notebook_path,
                        overwrite=True,
                        format=workspace.ImportFormat.SOURCE,
                        language=workspace.Language.PYTHON,
                        content=base64.b64encode((content).encode()).decode())
    job_name = "populate_cs_index"
    description = "Populates an index on Azure Search by splitting JSON documents from the blob container."
    task_key = "run_job"

    print("Attempting to create the job. Please wait...\n")
    base_parameters = { "endpoint": f"https://{args.searchservice}.search.windows.net", 
                        "index_name": f"{args.index}",
                        "connection_string": f"https://{args.storageaccount}.blob.core.windows.net",
                        "container_name": f"{args.container}" }
    j=w.jobs.create(
        job_name = job_name,
        tasks = [
            JobTaskSettings(
            description = description,
            existing_cluster_id = clusterid,
            notebook_task = NotebookTask(
                base_parameters = base_parameters,
                notebook_path = notebook_path,
                source = NotebookTaskSource("WORKSPACE")
            ),
            task_key = task_key
            )
        ]
    )

    r=w.jobs.run_now(
        job_id=j.job_id
    )
    print(f"Running job {j.job_id}. It will take about 15 minutes to complete the indexing.\n")
    print(f"View the job at {w.config.host}/#job/{j.job_id}\n"),

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

def create_search_index():
    if args.verbose: print(f"Ensuring search index {args.index} exists")
    index_client = SearchIndexClient(endpoint=f"https://{args.searchservice}.search.windows.net/",
                                     credential=search_creds)
    if args.index not in index_client.list_index_names():
        index = SearchIndex(
            name=args.index,
            fields=[
                SimpleField(name="id", type="Edm.String", filterable=True, facetable=True),
                SearchableField(name="content", type="Edm.String", analyzer_name="en.microsoft"),
                SimpleField(name="kind", type="Edm.String", filterable=True, facetable=True),
                SimpleField(name="category", type="Edm.String", filterable=True, facetable=True),
                SimpleField(name="sourcepage", type="Edm.String", filterable=True, facetable=True),
                SimpleField(name="sourcefile", type="Edm.String", filterable=True, facetable=True),
                SimpleField(name="keyfield", type="Edm.String", key=True)
            ],
            semantic_settings=SemanticSettings(
                configurations=[SemanticConfiguration(
                    name='default',
                    prioritized_fields=PrioritizedFields(
                        title_field=None, prioritized_content_fields=[SemanticField(field_name='content')]))])
        )
        if args.verbose: print(f"Creating {args.index} search index")
        index_client.create_index(index)
    else:
        if args.verbose: print(f"Search index {args.index} already exists")

if args.removeall:
    remove_blobs(None)
#    remove_from_index(None)
else:
    if not args.remove:
        create_search_index()

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
            if (not args.skipblobs == True):
                upload_blobs(filename)
            else:
                print('SKIPBLOBS = TRUE | Skipping upload of files from ./data')
            # page_map = get_document_text(filename)
            # sections = create_sections(os.path.basename(filename), page_map)
            # index_sections(os.path.basename(filename), sections)
    if (not args.skipindex == True): 
        populate_index_with_databricks()
    else:
        print('SKIPINDEX = TRUE | Skipping indexing')
print('It can take a few minutes for background indexing to finish even after the app is up and running. If your app cannot find any data when you ask questions, please wait a few minutes.')    
