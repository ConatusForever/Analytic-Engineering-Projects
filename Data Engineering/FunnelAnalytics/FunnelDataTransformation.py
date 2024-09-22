import pandas as pd, os
from functools import reduce

def creatRawDf():
   '''
   Creates one dataframe out of all csv files in the directory
   '''
   
   dataframes = [pd.read_csv(file) for file in os.listdir() if file.endswith('csv')]
   dataframes = [dataframes[i].rename(columns={'page':f'page_{i}'}) for i in range(len(dataframes))]
   mergedDf = (
    reduce(lambda left, right: pd.merge(left, right, on='user_id', how='left', suffixes=('_left', '_right')), dataframes)
)
   return mergedDf

def cleanDf(rawUserJourney):
    '''
    Takes in the raw merged user journey fact table and cleans it

    params (dataframe): Raw merged user journey dataframe

    returns (dataframe): Cleaned merged user journey dataframe
    '''


    userJourneyDf= (
    pd.melt(rawUserJourney, id_vars =['user_id', 'date', 'device', 'sex'] ,value_vars=['page_0', 'page_1', 'page_2','page_3'])
    .assign(date = lambda x: pd.to_datetime(x['date']))
    .drop(columns= 'variable')
    .rename(columns={'value':'page_type'})
    .fillna({'page_type':'no_activity'})
    .sort_values(by=['date'], ascending=True)
    .reset_index()
    .rename(columns={'index':'visit_Id'})
    )  

    return userJourneyDf

def createDimensions(userJourneyDataframe):
    '''
    Takes in the user journey dataframe and 
    creates dimension tables out of the 
    attribute columns    
    '''

    dimDevices= (
    userJourneyDataframe['device']
    .drop_duplicates()
    .reset_index(drop=True)
    .reset_index()
    .rename(columns={'index':'device_id'})
    .assign(device_id = lambda x: x['device_id'] +1000)
    )
    
    dimSexes=(
    userJourneyDataframe['sex']
    .drop_duplicates()
    .reset_index(drop=True)
    .reset_index()
    .rename(columns={'index':'sex_id'})
    .assign(sex_id = lambda x: x['sex_id'] + 2000)
    )

    dimPages=(
    userJourneyDataframe['page_type']
    .drop_duplicates()
    .reset_index(drop=True)
    .reset_index()
    .rename(columns={'index':'page_type_id'})
    .assign(page_type_id = lambda x: x['page_type_id'] + 3000)
    )

    minDate, maxDate = userJourneyDataframe['date'].min(), userJourneyDataframe['date'].max()
    dateRange = pd.date_range(minDate, maxDate)
    dimDate=(
    pd.DataFrame(dateRange, columns=['date'])
    .assign(month_name= lambda x: pd.to_datetime(x['date']).dt.strftime('%B'),
            month_short = lambda x: pd.to_datetime(x['date']).dt.strftime('%b'),
            month_number = lambda x: pd.to_datetime(x['date']).dt.strftime('%m'),
            day_of_week = lambda x: pd.to_datetime(x['date']).dt.strftime('%A'),
            day_as_number = lambda x: pd.to_datetime(x['date']).dt.strftime('%w'),
            year = lambda x: pd.to_datetime(x['date']).dt.strftime('%Y'),
             )
        )
    return  dimDevices, dimSexes, dimPages, dimDate

def mergeDimensionsToFact(userJourney, devicesDf, sexesDf, pagesDf):
    '''
    Merges dimension IDs into the fact table and removes descriptive columns.

    params:
    - userJourney (DataFrame): The fact table (user journey fact table).
    - devicesDf (DataFrame): The devices dimension table.
    - sexesDf (DataFrame): The sexes dimension table.
    - pagesDf (DataFrame): The pages dimension table.
    - dimDate (DataFrame): The dates dimension table.

    returns (DataFrame): The fact table with foreign keys and without descriptive columns.
    '''
    
    userJourney = userJourney.merge(devicesDf[['device', 'device_id']], on='device', how='left')

    userJourney = userJourney.merge(sexesDf[['sex', 'sex_id']], on='sex', how='left')

    userJourney = userJourney.merge(pagesDf[['page_type', 'page_type_id']], on='page_type', how='left')

    userJourney = userJourney.drop(columns=['device', 'sex', 'page_type'])

    return userJourney

def dataQualityCheck(userJourneyFactTable):
    '''
    Runs a data quality check on the user journey table

    params (dataframe): The user journey dataframe

    returns (string): A statement determining if all checks were passed
    '''
    expectedDataTypes= {'visit_Id':'int64',
                'user_id':'int64',
                'date': 'datetime64[ns]',
                'device':'object',
                'sex':'object',
                'page_type':'object'}

    # Missing Value Check
    def checkMissingValues(userjourneyFactDf):
       return userJourneyFactTable.isna().sum().sum()==0
        
    # Duplicate Rows Check    
    def checkForDuplicates(userjourneyFactDf):
        return userJourneyFactTable.duplicated().sum()==0
 
    # Data Types Check
    def checkDataTypes(userjourneyFactDf):
        dataTypeCheckList =[(expectedDtype, str(userJourneyFactTable[col].dtype)) for col, expectedDtype in expectedDataTypes.items()]
        return all(expected == actual for expected, actual in dataTypeCheckList)
        
    results = []
    results.append(('Missing Value Check', checkMissingValues(userJourneyFactTable)))
    results.append(('Duplicate Row Check', checkForDuplicates(userJourneyFactTable)))
    results.append(('Data Types Check', checkDataTypes(userJourneyFactTable)))

    if all(result[1] for result in results):
        dataQualityResult = 'Passed'
    else:
        dataQualityResult = 'Failed'

    return dataQualityResult, results
    

def main():
    '''
    The main portion of the program
    
    '''

    uncleanUserJourney = creatRawDf() # creates a merged dataframe
    print('Inital Merge Complete')

    userJourney= cleanDf(uncleanUserJourney) # cleans the merged dataframe
    print('Data Cleaning Complete')

    dataQualityCheckResults = dataQualityCheck(userJourney)[0] # runs a data quality test on cleaned user journey data
    if dataQualityCheckResults == 'Passed':
        print(f'Data Quality Check: {dataQualityCheckResults}')
    else:
        raise ValueError('Data Quality Check: Failed')
    
    
    devicesDf, sexesDf, pagesDf, dateDf = createDimensions(userJourney) # create dimension tables
    
     # Merge dimension IDs into the fact table and remove descriptive columns
    userJourney = mergeDimensionsToFact(userJourney, devicesDf, sexesDf, pagesDf)

    # save the fact and dimension tables
    dataModel = {'user journey':userJourney, 
             'devices': devicesDf,
              'sexes': sexesDf,
               'pages': pagesDf,
                'dates': dateDf}
    
    return dataModel

main()