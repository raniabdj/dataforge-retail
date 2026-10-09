from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

STORAGE_ACCOUNT = "dataforgelake2026"
FILE_SYSTEM = "dataforge"

# Authenticate using your existing Azure CLI login
credential = AzureCliCredential()

# Connect to Azure Data Lake Storage Gen2
service_client = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
    credential=credential,
)

file_system_client = service_client.get_file_system_client(FILE_SYSTEM)

print("Connected to Azure Data Lake!")

# List files in the Bronze layer
print("\nBronze files:")

for path in file_system_client.get_paths(path="bronze"):
    if not path.is_directory:
        print(f" - {path.name}")