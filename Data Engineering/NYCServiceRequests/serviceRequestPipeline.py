import pandas as pd, re, os, storageSecrets
from azure.storage.blob import BlobServiceClient




def getData():
    '''
    Crawls the directory, gets the Service Request files, and creates a combined dataframe.

    Returns:
        serviceRequests (DataFrame): A combined dataframe of all the csv files.
    '''
    filePaths = [path for path in os.listdir() if re.search('ServiceRequests', path)] # get paths
    combinedDF = pd.DataFrame()

    for path in filePaths:                     # create dataframe 
        serviceRequestsDF = pd.read_csv(path, low_memory=False)
        combinedDF = pd.concat([combinedDF ,serviceRequestsDF], axis=0)

    return combinedDF

serviceRequests = getData()


serviceRequests.to_parquet('serviceRequests.gzip', engine='pyarrow')

def uploadToBlobStorage(filePath, fileName):
    """
    Uploads a file to Azure Blob Storage.

    Args:
        filePath (str): The local file path of the file to be uploaded.
        fileName (str): The name of the file to be uploaded.

    Returns:
        None
    """
    
    connectionString = storageSecrets.connectionString
    containerName = storageSecrets.containerName

    blobServiceClient = BlobServiceClient.from_connection_string(connectionString)
    blobClient = blobServiceClient.get_blob_client(container=containerName, blob=fileName)

    with open(filePath, 'rb') as data:
        blobClient.upload_blob(data)

    print(f'uploaded {fileName} file')



uploadToBlobStorage('serviceRequests.gzip', 'serviceRequest')