import requests, pandas as pd, re, concurrent.futures, numpy as np
from bs4 import BeautifulSoup

endpoints =[f'https://www.yellowpages.com/search?geo_location_terms=New+York+City%2C+NY&search_terms=restaurants&page={i}' for i in range(1,11)]

def scrapeYellowPages(url):
    '''
    1. This function takes in a list of endpoints. 
    2. Loops through all urls.
    3. Creates a list of dictionary items with restuarant data

    Parameters:
        endpoints (str) : The list of yellow pages endpoints
    
    Returns:
        restaurantDict (list): A list of dictionary objects containing restaurant info
    '''

    businessNames = []
    restaurantType = []
    phoneNumbers =[]
    addresses =[]
    yearsInBusiness =[]
    dollarCosts =[]
    websites = []
    menuLinks = []
    tripadvisorRatings = []
    orderOnlineStatus = []
    yellowpagesRatings = []
    secondaryInfo = []



    # for url in urls:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Get Addresses
    for i in soup.find_all(class_='info-section info-secondary'):
        addresses.append(i.find_all('div',class_='adr')[0].text)

    # Get Business Name

    for i in soup.find_all(class_ ='info-section info-primary'):
        businessNames.append(i.find(class_='n').find('a').text)

    # Get Restaurant Type
    for i in soup.find_all(class_='info-section info-primary'):
        restaurantType.append(i.find_all(class_='categories')[0].text)
    
    # Get Phone Numbers
    for i in soup.find_all(class_='info-section info-secondary'):
        phoneNumbers.append(i.find_all('div',class_='phones phone primary')[0].text)

    # Price Range
    for i in soup.find_all(class_='info-section info-secondary'):
        try:
            dollarCosts.append(i.find_all('div',class_='price-range')[0].text)
        except:
            dollarCosts.append('None')

    # Years in Business
    for i in soup.find_all(class_='info-section info-primary'):
        try:
            yearsInBusiness.append(i.find_all(class_='badges')[0].find_all('strong')[0].text.strip('Years'))
        except:
            yearsInBusiness.append('None')
    
    # Get Website
    for i in soup.find_all(class_='info-section info-primary'):
        try:
            websites.append(i.find_all(class_='track-visit-website')[0]['href'])
        except:
            websites.append('None')

    # Get Menu URL
    for i in soup.find_all(class_='info-section info-primary'):
        try:
            remaining = i.find_all(class_='menu')[0]['href']
            menuLinks.append(f'http:/{remaining}')
        except:
            menuLinks.append('None')  
    
    # Get tripadvisor ratings
    
    for i in soup.find_all(class_ ='info-section info-primary'):
        try:
            ratings = i.find_all('div', class_='ratings')[0]['data-tripadvisor']
            decimal_number = re.search(r'\d+\.\d+', ratings).group()
            tripadvisorRatings.append(decimal_number)
        except:
            tripadvisorRatings.append('None')

    # # Yellow Pages Ratings
    for i in soup.find_all('div', class_='info-section info-primary'):
        try:
            rating = re.search(r'class="(.+?)">',str(i.find_all('a', class_='rating hasExtraRating')[0].find_all('div')[0])).group(1)
            yellowpagesRatings.append(' '.join(rating.split(' ')[1:]))
        except:
            yellowpagesRatings.append('None')

    # Get Order Online
    for i in soup.find_all('div', class_='info'):
        try:
            orderOnlineStatus.append(i.find_all('a', class_='action order-online')[0].text)
        except:
            orderOnlineStatus.append('None')
    
    # SecondaryInfo
    for i in soup.find_all('div', class_='info'):
        try:
            secondaryInfo.append(i.find_all('p')[0].text)
        except:
            secondaryInfo.append('None')            


        restaurantDict = {
            'businessNames': businessNames,
            'restaurantType': restaurantType,
            'phoneNumbers': phoneNumbers,
            'addresses': addresses,
            'yearsInBusiness': yearsInBusiness,
            'dollarCosts': dollarCosts,
            'websites': websites,
            'menuLinks': menuLinks,
            'tripAdvisorRatings': tripadvisorRatings,
            'orderOnlineStatus': orderOnlineStatus,
            'yellowpagesRatings': yellowpagesRatings,
            'secondaryInfo': secondaryInfo
            }

    return restaurantDict


def createDataframe():
    '''
    This function loops through the list of restaurant dictionaries,
    creates a dataframe out of them, and cleans the dataframe by performing various transformations.

    Returns:
        cleanRestaurantsDF (pandas.DataFrame): The combined and cleaned restaurant dictionaries as a dataframe.
    '''

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:         # multi-threading to speed up program
        yellowPages = list(executor.map(scrapeYellowPages,endpoints))
        
    restaurantsDF= pd.DataFrame()
    for restaurant in yellowPages:          # Create dataframe
        newRestaurantDF = pd.DataFrame(restaurant)
        restaurantsDF = pd.concat([restaurantsDF, newRestaurantDF])


    ypRatingMap = {
                'None': np.NaN,
               'one': 1,
               'one half': 1.5,
               'two': 2,
               'two half': 2.5,
               'three': 3,
               'three half':3.5,
               'four': 4,
               'four half': 4.5,
               'five': 5
               }

    dollarCostsMap = {
        '$': 'Very Cheap',
        '$$': 'Cheap',
        '$$$': 'Fair',
        '$$$$': 'Expensive',
        '$$$$$': 'Very Expensive'
    }

    cleanRestaurantsDF = (     # Cleaning dataframe
        restaurantsDF
        .assign(restaurantType = lambda x: x['restaurantType'].str.replace(r'Restaurants', '', 1), 
            secondaryInfo = lambda x: x['secondaryInfo'].str.replace('From Business:', ''),
            yellowpagesRatings = lambda x: x['yellowpagesRatings'].map(ypRatingMap).astype(float), 
            yearsInBusiness = lambda x: x['yearsInBusiness'].str.replace('None', '').replace('', np.NaN).astype(float),
            tripAdvisorRatings = lambda x: x['tripAdvisorRatings'].str.replace('None', '').replace('', np.NaN).astype(float),
            dollarCosts = lambda x: x['dollarCosts'].map(dollarCostsMap))
        .reset_index()
        .assign(ypKey = lambda x: x['index'] + 1100)
        .drop(columns='index')
    )

    
    return cleanRestaurantsDF

yellowPagesRestaurantsDF = createDataframe()