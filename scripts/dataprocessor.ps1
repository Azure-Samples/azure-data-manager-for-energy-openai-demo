Write-Host ""
Write-Host "Loading azd .env file from current environment"
Write-Host ""

$output = azd env get-values

foreach ($line in $output) {
  if (!$line.Contains('=')) {
    continue
  }

  $name, $value = $line.Split("=")
  $value = $value -replace '^\"|\"$'
  [Environment]::SetEnvironmentVariable($name, $value)
}

Write-Host "Environment variables set."

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
  # fallback to python3 if python not found
  $pythonCmd = Get-Command python3 -ErrorAction SilentlyContinue
}

Write-Host 'Creating python virtual environment "scripts/.venv"'
Start-Process -FilePath ($pythonCmd).Source -ArgumentList "-m venv ./scripts/.venv" -Wait -NoNewWindow

$venvPythonPath = "./scripts/.venv/scripts/python.exe"
if (Test-Path -Path "/usr") {
  # fallback to Linux venv path
  $venvPythonPath = "./scripts/.venv/bin/python"
}

$databricksIdentity = az identity show --name dbmanagedidentity --resource-group $env:AZURE_DATABRICKS_MANAGED_RESOURCE_GROUP | ConvertFrom-Json
$databricksIdentityId = $databricksIdentity.principalId
$searchroles = @(
  "5e0bd9bd-7b93-4f28-af87-19fc36ad61bd",
  "1407120a-92aa-4202-b7e9-c0e197c71c8f",
  "8ebe5a00-799e-43f5-93ac-243d3dce84a7"
)

$storageroles = @(
  "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1",
  "ba92f5b4-2d11-453d-a403-e96b0029c9fe"
)

Write-Host 'Running Databricks role assignments'

foreach ($role in $searchroles) {
  $roleassignment = az role assignment create `
      --role $role `
      --assignee-object-id $databricksIdentityId `
      --scope /subscriptions/$env:AZURE_SUBSCRIPTION_ID/resourceGroups/$env:AZURE_SEARCH_SERVICE_RESOURCE_GROUP `
      --assignee-principal-type ServicePrincipal | ConvertFrom-Json
  $roleassignmentname = $roleassignment.roleDefinitionName
  Write-Host "Added Databricks Identity ($databricksIdentityId) added to role $roleassignmentname on $env:AZURE_SEARCH_SERVICE_RESOURCE_GROUP"
}

foreach ($role in $storageroles) {
  $roleassignment = az role assignment create `
      --role $role `
      --assignee-object-id $databricksIdentityId `
      --scope /subscriptions/$env:AZURE_SUBSCRIPTION_ID/resourceGroups/$env:AZURE_STORAGE_RESOURCE_GROUP `
      --assignee-principal-type ServicePrincipal | ConvertFrom-Json
      $roleassignmentname = $roleassignment.roleDefinitionName
  Write-Host "Added Databricks Identity ($databricksIdentityId) added to role $roleassignmentname on $env:AZURE_STORAGE_RESOURCE_GROUP"
}

Write-Host 'Installing dependencies from "requirements.txt" into virtual environment'
Start-Process -FilePath $venvPythonPath -ArgumentList "-m pip install -r ./scripts/requirements.txt" -Wait -NoNewWindow

Write-Host 'Running "dataprocessor.py"'
$cwd = (Get-Location)
$startArgs = ""
$startArgs += "./scripts/dataprocessor.py "
$startArgs += "$cwd/data/TNO/* "
$startArgs += "--storageaccount $env:AZURE_STORAGE_ACCOUNT "
$startArgs += "--container $env:AZURE_STORAGE_CONTAINER "
$startArgs += "--searchservice $env:AZURE_SEARCH_SERVICE "
$startArgs += "--index $env:AZURE_SEARCH_INDEX "
$startArgs += "--tenantid $env:AZURE_TENANT_ID "
$startArgs += "--databricksworkspaceurl $env:AZURE_DATABRICKS_WORKSPACE_URL "
$startArgs += "--databricksworkspaceid $env:AZURE_DATABRICKS_WORKSPACE_ID "
$startArgs += "-v"
if ($env:SKIPBLOBS -eq "TRUE") {
  $startArgs += "--skipblobs"
}
if ($env:SKIPINDEX -eq "TRUE") {
  $startArgs += "--skipindex"
}

Start-Process -FilePath $venvPythonPath -ArgumentList  $startArgs -Wait -NoNewWindow
azd env set SKIPBLOBS "TRUE"
azd env set SKIPINDEX "TRUE"