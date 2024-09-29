import requests, pandas as pd
from bs4 import BeautifulSoup

ourAirports = 'https://ourairports.com/data/'

def getAirportData(url):
  '''
  This function crawls the Our Airports website
  and gets the paths to all datasets
  
  Parameters:
    URL (str): The Our airports link to scrape

  Returns:
    datasets (list): A list of dataframes for the airport data
  
  '''
  response = requests.get(ourAirports)
  soup = BeautifulSoup(response.content, 'html.parser')
  
  downloads = soup.find_all('dt')
  
  paths = [path.text.split('(')[0].split('.')[0].replace('\n','') for path in downloads] # get paths
  links = [link.find_all('a', href=True)[0]['href'] for link in downloads] # get links
  dataframes = [pd.read_csv(link) for link in links] # get dataframes
  

  return list(zip(paths, dataframes))
  
listOfDataframesAndNames = getAirportData(ourAirports)

