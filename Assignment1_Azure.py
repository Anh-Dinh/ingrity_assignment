
import os
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential

def upload_file_to_adls(storage_account_name, storage_account_key, container_name, local_file_path, adls_file_path):
    try:
        service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net",
                                               credential=storage_account_key)

        file_system_client = service_client.get_file_system_client(file_system=container_name)

        directory_client = file_system_client.get_directory_client(os.path.dirname(adls_file_path))
        file_client = directory_client.create_file(os.path.basename(adls_file_path))

        with open(local_file_path, "rb") as file:
            file_contents = file.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))

        print(f"File {local_file_path} uploaded to {adls_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError

def upload_file_to_adls(storage_account_name, storage_account_key, container_name, local_file_path, adls_file_path):
    try:
        # Create a DataLakeServiceClient
        service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net",
                                               credential=storage_account_key)

        # Create a container (file system) if it doesn't exist
        try:
            file_system_client = service_client.create_file_system(file_system=container_name)
            print(f"Container '{container_name}' created.")
        except ResourceExistsError:
            file_system_client = service_client.get_file_system_client(file_system=container_name)
            print(f"Container '{container_name}' already exists.")

        # Create a directory if it doesn't exist
        directory_path = os.path.dirname(adls_file_path)
        if directory_path:
            directory_client = file_system_client.create_directory(directory_path)
            print(f"Directory '{directory_path}' created or already exists.")

        # Create a file client
        file_client = file_system_client.get_file_client(adls_file_path)

        # Upload the file
        with open(local_file_path, "rb") as file:
            file_contents = file.read()

        file_client.upload_data(data=file_contents, overwrite=True)

        print(f"File {local_file_path} uploaded to {adls_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Usage
storage_account_name = ""
storage_account_key = ""
container_name = "sample-container"
local_file_path = "sample_data.csv"
adls_file_path = "sample_data.csv"

upload_file_to_adls(storage_account_name, storage_account_key, container_name, local_file_path, adls_file_path)
