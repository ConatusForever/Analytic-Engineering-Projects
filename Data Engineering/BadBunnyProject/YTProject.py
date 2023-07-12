# Bad Bunny YouTube Data Project

# import libraries
from googleapiclient.discovery import build
import pandas as pd, requests, youTubeCredentials
from IPython.display import JSON

# function build api service

api_key = youTubeCredentials.youTubeApi #api key
api_service_name = "youtube" #api service name
api_version = "v3" #api version


# YouTube API Client
def build_api_service(service_name, version, key):
    ''' This function takes in the required parameters 
        and returns a YouTube API Client'''
    
    return build(api_service_name, api_version, developerKey=api_key)

youtube = build_api_service(api_service_name, api_version, api_key)




