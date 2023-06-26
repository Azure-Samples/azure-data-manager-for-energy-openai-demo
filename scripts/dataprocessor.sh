 #!/bin/sh

echo ""
echo "Loading azd .env file from current environment"
echo ""

while IFS='=' read -r key value; do
    value=$(echo "$value" | sed 's/^"//' | sed 's/"$//')
    export "$key=$value"
done <<EOF
$(azd env get-values)
EOF

echo 'Creating python virtual environment "scripts/.venv"'
python -m venv scripts/.venv

echo 'Installing dependencies from "requirements.txt" into virtual environment'
./scripts/.venv/bin/python -m pip install -r scripts/requirements.txt

databricksIdentity=$(az identity show --name dbmanagedidentity --resource-group $AZURE_DATABRICKS_MANAGED_RESOURCE_GROUP | jq -r '.principalId')
searchroles="5e0bd9bd-7b93-4f28-af87-19fc36ad61bd 1407120a-92aa-4202-b7e9-c0e197c71c8f 8ebe5a00-799e-43f5-93ac-243d3dce84a7"
storageroles="2a2b9908-6ea1-4ae2-8e65-a410df84e7d1 ba92f5b4-2d11-453d-a403-e96b0029c9fe"

echo "Running Databricks role assignments"

for role in $searchroles
do
  roleassignment=$(az role assignment create \
    --role "$role" \
    --assignee-object-id "$databricksIdentity" \
    --scope "/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$AZURE_SEARCH_SERVICE_RESOURCE_GROUP" \
    --assignee-principal-type ServicePrincipal | jq -r '.roleDefinitionName')
  echo "Added Databricks Identity ($databricksIdentity) added to role $roleassignment on $AZURE_SEARCH_SERVICE_RESOURCE_GROUP"
done

for role in $storageroles
do
  roleassignment=$(az role assignment create \
    --role "$role" \
    --assignee-object-id "$databricksIdentity" \
    --scope "/subscriptions/$AZURE_SUBSCRIPTION_ID/resourceGroups/$AZURE_STORAGE_RESOURCE_GROUP" \
    --assignee-principal-type ServicePrincipal | jq -r '.roleDefinitionName')
  echo "Added Databricks Identity ($databricksIdentity) added to role $roleassignment on $AZURE_STORAGE_RESOURCE_GROUP"
done


echo 'Running "dataprocessor.py"'

if [ "$SKIPBLOBS" = "TRUE" ]; then
  skipblobs="--skipblobs"
fi

if [ "$SKIPINDEX" = "TRUE" ]; then
  skipindex="--skipindex"
fi

./scripts/.venv/bin/python ./scripts/dataprocessor.py './data/TNO/*' --storageaccount "$AZURE_STORAGE_ACCOUNT" --container "$AZURE_STORAGE_CONTAINER" --searchservice "$AZURE_SEARCH_SERVICE" --index "$AZURE_SEARCH_INDEX" --tenantid "$AZURE_TENANT_ID" --databricksworkspaceurl "$AZURE_DATABRICKS_WORKSPACE_URL" --databricksworkspaceid "$AZURE_DATABRICKS_WORKSPACE_ID" -v $skipblobs $skipindex



azd env set SKIPBLOBS "TRUE"
azd env set SKIPINDEX "TRUE"