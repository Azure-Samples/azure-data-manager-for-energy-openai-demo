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

echo 'Running "dataprocessor.py"'
startArgs=""
startArgs+="$cwd/data/TNO/* "
startArgs+="--storageaccount $AZURE_STORAGE_ACCOUNT "
startArgs+="--container $AZURE_STORAGE_CONTAINER "
startArgs+="--searchservice $AZURE_SEARCH_SERVICE "
startArgs+="--index $AZURE_SEARCH_INDEX "
startArgs+="--tenantid $AZURE_TENANT_ID "
startArgs+="--databricksworkspaceurl $AZURE_DATABRICKS_WORKSPACE_URL "
startArgs+="--databricksworkspaceid $AZURE_DATABRICKS_WORKSPACE_ID "
startArgs+="-v"

if [ "$SKIPBLOBS" = "TRUE" ]; then
  startArgs+=" --skipblobs"
fi

if [ "$SKIPINDEX" = "TRUE" ]; then
  startArgs+=" --skipindex"
fi

./scripts/.venv/bin/python ./scripts/dataprocessor.py $startArgs

azd env set SKIPBLOBS "TRUE"
azd env set SKIPINDEX "TRUE"