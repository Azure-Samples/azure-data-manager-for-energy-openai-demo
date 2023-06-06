import os
from dotenv import load_dotenv
from typing import Dict
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
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
import json

# Get the service endpoint and API key from the environment
endpoint = os.environ["COGNITIVE_SEARCH_INSTANCE_URL"]
key = os.environ["COGNITIVE_SEARCH_KEY"]

## Create a service client
client = SearchIndexClient(endpoint, AzureKeyCredential(key))

from azure.search.documents.indexes.models import SimpleField, SearchFieldDataType

## New code
fields = []
simple_field = SimpleField(name="id", type=SearchFieldDataType.String, key=False, facetable=True, filterable=True)
fields.append(simple_field)
key_field = SimpleField(name="keyfield", type=SearchFieldDataType.String, key=True)
fields.append(key_field)
simple_field = SearchableField(name="content", type=SearchFieldDataType.String, key=False)
fields.append(simple_field)
simple_field = SimpleField(name="category", type=SearchFieldDataType.String, key=False, facetable=True, filterable=True)
fields.append(simple_field)
simple_field = SimpleField(name="sourcepage", type=SearchFieldDataType.String, key=False, facetable=True, filterable=True)
fields.append(simple_field)
simple_field = SimpleField(name="sourcefile", type=SearchFieldDataType.String, key=False, facetable=True, filterable=True)
fields.append(simple_field)


# Create the index
name = os.environ["COGNITIVE_SEARCH_INDEX_NAME"]

cors_options = CorsOptions(allowed_origins=["*"], max_age_in_seconds=60)
scoring_profiles = []

index = SearchIndex(
    name=name,
    fields=fields,
    scoring_profiles=scoring_profiles,
    cors_options=cors_options)

result = client.create_index(index)