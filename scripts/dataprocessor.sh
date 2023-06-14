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
if [ $SKIPBLOBS == 'TRUE' ]
then
    echo 'TRUE'
    #./scripts/.venv/bin/python ./scripts/dataprocessor.py '$cwd/data/TNO/*' --storageaccount $AZURE_STORAGE_ACCOUNT --container $AZURE_STORAGE_CONTAINER --searchservice $AZURE_SEARCH_SERVICE --index $AZURE_SEARCH_INDEX --tenantid $AZURE_TENANT_ID --databricksworkspaceurl $AZURE_DATABRICKS_WORKSPACE_URL --skipblobs -v
else
    echo 'FALSE'
    #./scripts/.venv/bin/python ./scripts/dataprocessor.py '$cwd/data/TNO/*' --storageaccount $AZURE_STORAGE_ACCOUNT --container $AZURE_STORAGE_CONTAINER --searchservice $AZURE_SEARCH_SERVICE --index $AZURE_SEARCH_INDEX --tenantid $AZURE_TENANT_ID --databricksworkspaceurl $AZURE_DATABRICKS_WORKSPACE_URL -v
fi
azd env set SKIPBLOBS "TRUE"