''' This program will take in the complaints dataset and clean it'''



import pandas as pd

# Reading CSV File into a DataFrame

print('Reading CSV file into a DataFrame...')
complaints = pd.read_csv('customer_complaints.csv')
complaints2 = complaints.copy()


def cleaning_data(complaints_data):
    '''This function will clean the data'''

    print('Data processing initiated...')
    print(f'Number of rows: {"{:,}".format(complaints_data.shape[0])}')
    print(f'Number of columns: {complaints_data.shape[1]}')
    print('Cleaning data...')

    
    complaints_data = (
        complaints_data
        .assign(Date_received  = pd.to_datetime(complaints_data['Date received']))
        .assign(Date_sent_to_company = pd.to_datetime(complaints_data['Date sent to company']))
        .reindex(columns = ['Complaint ID', 'Date_received', 'Product', 'Sub-product', 'Issue', 'Sub-issue', 'Company public response', 'Company', 'State', 'ZIP code', 'Consumer consent provided?', 'Submitted via', 'Date_sent_to_company', 'Company response to consumer', 'Timely response?', 'Consumer disputed?'])
        
    )

    #Filling in Missing Values
    complaints_data.fillna('Missing', inplace=True)

    print('Data Processing Complete')
    return complaints_data

def main():
    consumer_complaints = cleaning_data(complaints2)
    print('Saving cleaned data to a CSV file...')
    consumer_complaints.to_csv('consumer_complaints_cleaned.csv', index=False)
    print('Saved')
    return consumer_complaints

main()
