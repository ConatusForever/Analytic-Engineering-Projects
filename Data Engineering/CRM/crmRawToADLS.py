import pandas, os
from azure.storage.blob import BlobServiceClient

os.chdir('C:\\Users\\hakee\\OneDrive\\Data Engineering\\DE Projects\\CRM')

with open('analyticsengSecrets.txt', 'r') as file:
    data = file.readlines()

key = data[2].split('=')[1].strip()
containerName = data[-1].split('=')[1].strip()
connectionString = data[-3].split('=',1)[1].strip()

os.chdir('CRM+Sales+Opportunities')

csvList = [i for i in os.listdir() if not i.endswith('dictionary.csv') and not i.endswith('.py')]
def uploadToBlobStorage(fileList):
    """
    Uploads a file to Azure Blob Storage.

    Args:
        csvList (list): A list of file paths of the files to be uploaded.

    Returns:
        None
    """


    for file in fileList:

        blobServiceClient = BlobServiceClient.from_connection_string(connectionString)
        blobClient = blobServiceClient.get_blob_client(container=containerName, blob=file)

        with open(file, 'rb') as data:
            blobClient.upload_blob(data)

        print(f'uploaded {file.split('.')[0]} file')

uploadToBlobStorage(csvList)