import snowflake.connector, snowflake_config, FunnelDataTransformation, pandas
from snowflake.connector.pandas_tools import write_pandas

def helpItems(funnelData):
    '''
    This function processes a dictionary of dataframes (funnelData) by converting all column names 
    in each dataframe to uppercase. The function then prepares a list of dataframe names and 
    corresponding dataframes.

    Parameters:
        funnelData (dict): A dictionary containing the dataframes for 'user journey', 'sexes', 
                        'devices', 'pages', and 'dates'. The keys should be the names of these 
                        dataframes, and the values should be pandas DataFrames.

    Returns:
        tuple: 
            - dataframeNames (list of str): A list of dataframe names ('SEXES', 'DEVICES', 'PAGES', 
            'DATES', 'USER_JOURNEY') in uppercase.
            - dataModelList (list of DataFrame): A list of the corresponding pandas DataFrames with 
            their column names converted to uppercase.
    '''

    userJourneyDf = funnelData.get('user journey')
    userJourneyDf.columns = userJourneyDf.columns.str.upper()
    sexesDf = funnelData.get('sexes')
    sexesDf.columns = sexesDf.columns.str.upper()
    devicesDf = funnelData.get('devices')
    devicesDf.columns = devicesDf.columns.str.upper()
    pagesDf = funnelData.get('pages')
    pagesDf.columns = pagesDf.columns.str.upper()
    datesDf = funnelData.get('dates')
    datesDf.columns = datesDf.columns.str.upper()

    dataframeNames =['SEXES','DEVICES', 'PAGES', 'DATES', 'USER_JOURNEY']
    dataModelList = [sexesDf, devicesDf, pagesDf, datesDf, userJourneyDf]

    return dataframeNames, dataModelList

def createDBConnection():
    '''
    Create a snowflake db connection 
    with my credentials
    '''

    try:
        # Connect to Snowflake using the variables from snowflake_config
        conn = snowflake.connector.connect(
        user=snowflake_config.SNOWFLAKE_AEUSERNAME,
        password=snowflake_config.SNOWFLAKE_AEPASSWORD,
        account= snowflake_config.SNOWFLAKE_AEID,
        warehouse=snowflake_config.SNOWFLAKE_AEWAREHOUSE,
        database=snowflake_config.SNOWFLAKE_AEDB,
        role=snowflake_config.SNOWFLAKE_AEROLE
        )

        # Create a cursor object
        cs = conn.cursor()
        try:
            cs.execute('SELECT CURRENT_VERSION();')
            data = cs.fetchall()
            print(f'Snowflake Version: {data}')

        except Exception as e:
            print(f'Failed Connection: {e}')

        finally:
            cs.close()

        return conn
    
    except Exception as e:
        print(f'Error establishing connection {e}')
        return None   
    
def funnelAnalyticsSetup(connectionCursor):

    '''
    Creates the funnel analytics schema & tables
    '''

    connectionCursor.execute('CREATE OR REPLACE SCHEMA FunnelAnalytics;')
    
    connectionCursor.execute('''CREATE OR REPLACE TABLE devices(
                device_id int primary key,
                device varchar(10)
    );''')

    connectionCursor.execute('''CREATE OR REPLACE TABLE sexes(
                sex_id int primary key,
                sex varchar(10)
    );''')

    connectionCursor.execute('''CREATE OR REPLACE TABLE pages(
                page_type_id int primary key,
                page_type varchar(25)
    );''')

    connectionCursor.execute('''CREATE OR REPLACE TABLE dates(
                date timestamp,
                month_name varchar(15),
                month_short varchar(3),
                month_number int,
                day_of_week varchar(10),
                day_as_number int,
                year int
    );''')
 
    connectionCursor.execute('''CREATE OR REPLACE TABLE FunnelAnalytics.User_Journey(
                visit_Id int,
                user_id int,
                date timestamp,
                device_id int,
                sex_id int,
                page_type_id int,
                primary key (visit_id),
                foreign key (device_id) references FunnelAnalytics.devices(device_id),
                foreign key (sex_id) references FunnelAnalytics.sexes(sex_id),
                foreign key (page_type_id) references FunnelAnalytics.pages(page_type_id)
                             );''')

    

    return print('Snowflake Stage & Tables are created')


def pushToTables(dataframes: list, connection: str, tableNames: list):
    '''
    Function to load data into snowflake tables

    Params:
    - dataframes (List[pd.DataFrame]): List of DataFrames to load.
    - connection (str): Snowflake connection object.
    - tablenames (List[str]): List of corresponding table names.

    Returns:
    - (str): A message confirming whether data has been loaded successfully.
    '''

    print('Loading Data...')
    for name, table in zip(tableNames, dataframes):
        write_pandas(
            conn= connection,
            df = table,
            table_name= name,
            schema= 'FUNNELANALYTICS'
            
        )

    return print('Data has been loaded to all tables')

def main():
    '''
    The main part of the program
    '''

    funnelData = FunnelDataTransformation.main() # import dimension & fact tables
    dataframeNames, dataModelList = helpItems(funnelData) # list of dataframes and their names
    conn = createDBConnection() # create snowflake connection
    cs = conn.cursor() # 
    funnelAnalyticsSetup(cs) # create schema & tables
    pushToTables(dataModelList, conn, dataframeNames)

    # Close the cursor and connection
    cs.close()
    conn.close()

    print('Data Pipeline is complete!!!')

main()