
# Data Provided by: Metro Atlanta Evictions Data Collective
# Raymond, EL; Stein, S; Haley, V.; Woodworth, E; Zhang, G.; 
# Siva, R; Guhathakurta, S. Metro Atlanta Evictions Data Collective Database: Version 1.0. 
# School of City and Regional Planning: Georgia Institute of Technology, 2020, https://metroatlhousing.org/atlanta-region-eviction-tracker/.


import pandas as pd, requests, json, sql_server_info, schedule, time
from sqlalchemy import create_engine

url_day = 'https://atl-eviction-tracker.herokuapp.com/rest/tractdaily'


def get_data(url):
    ''' This function gets eviction data
    from the Federal Reserve Bank of Atlanta API'''

    print('Job Running...')
    try:
        response = requests.get(url)
        if response.status_code == 200:
            json_data = response.json()
            print('Success: Data downloaded.')
        else:
            print(f'Error: {str(response.status_code)} ')
    except:
        print('Error: Unable to get data.')
        
    return json_data




def convert_json(json):
    '''This function converts the json data
    into a dataframe'''
    
    # map for county data

    counties = {'121':'Fulton',
            '089': 'Dekalb',
            '067':'Cobb',
            '135': 'Gwinnett',
            '063':'Clayton'}

    df_daily = (
                pd.json_normalize(json)
                .assign(counties= lambda x: x['CountyID'].replace(counties))
                .assign(mnth = lambda x: x['FilingDate'].str.split('/').str[0].astype(int))
                .assign(yr = lambda x: x['FilingDate'].str.split('/').str[2].astype(int))
                .assign(mnth_and_yr = lambda x: x['mnth'].astype(str) + '/' + x['yr'].astype(str))
    )
    
    return df_daily


def write_to_db(dataframe):
    ''' This funciton writes the dataframe 
    to my sql server database.'''

        
    driver = sql_server_info.driver
    datasource = sql_server_info.datasource
    server_name = sql_server_info.server_name
    database_name = 'Evictions'


    conn_string = f'mssql+pyodbc://@{server_name}/{database_name}?driver={driver}'
    engine = create_engine(conn_string)
    conn = engine.connect()
    dataframe.to_sql('Daily_Evictions', con = conn, if_exists='append', index=False)



def main():
    ''' This function is the main function 
    that calls all the other functions in the program.'''
    
    eviction_json = get_data(url_day)
    eviction_df = convert_json(eviction_json)
    print('Writing to database...')
    write_to_db(eviction_df)
    

    return print('Done writing to database.')



main()
