
#The Fortune 1000 are the 1,000 largest American companies ranked by revenues, as compiled by the American business magazine Fortune

#### Here is our guiding questions for this analysis: ####

# 1. Which sectors are represented in the dataset?  
# 2. Which sector has the highest number of companies? Which one has the least? 
# 3. Who are the top 5 companies in terms of revenue? What is the difference from the previous company? 
# 4.  What’s the average revenue across all companies in the dataset? What is the average revenue for the top 5 companies with the highest revenue? Get the median for the top 5 companies with the highest revenue. What is the pct_change from the average? What is the pct_change from the median? 
# 5. Who are the top 5 companies in terms of profit? Among the top five revenue-generating companies, which ones also rank within the top five in terms of profitability? 
# 6. Which company has the lowest profit?  How many companies have negative profit?     
# 7. Which state has the highest number of Fortune 1000 companies? 
# 8. Which state has the highest combined revenue from all its Fortune 1000 companies? 
# 9. Is there a correlation between the number of employees a company has and its revenue? 
# 10. What percentage of the total revenue does each sector contribute to? 

import pandas as pd, matplotlib.pyplot as plt, numpy as np, seaborn as sns

companies = pd.read_csv('fortune1000.csv')
companies2 = companies.copy()

# Data Validation & Preprocessing

print("Lets take a look at random 15 rows, each variable's datatype then duplicate rows.")
print(companies2.sample(15))

print(companies2.info())

print(f'Duplicated rows: {companies2.duplicated().sum()}')

dupList = (
    companies2
    .assign(IsDuplicate = lambda x: x.duplicated())
    .query('IsDuplicate == True')['Company']
    .to_list()
)

dups =(
    companies2
    .query('Company in @dupList')
)
# Viewing duplicate rows
print(dups)

companies2 = companies2.drop_duplicates()
print(f'Duplicated rows: {companies2.duplicated().sum()}')

# splitting the location column into City & State
companies2 = (
    companies2
    .assign(City = lambda x: x['Location'].str.split(',',expand=True)[0],
            State= lambda x: x['Location'].str.split(',',expand=True)[1])
    .drop(columns='Location')    
)

print(companies2.head())

# Now that we've cleaned our data, let analyze it

# 1. Which sectors are represented in the dataset? 

print(list(companies2['Sector'].unique()))


# 2. Which sector has the highest number of companies? Which one has the least?

# Displaying the top 10 Sectors by Company
(
    companies2
    .groupby('Sector')['Company']
    .count()
    .sort_values(ascending=True)
    .iloc[:11]  
).plot(kind='barh') 
plt.title('Top 10 Sectors by Company', fontsize=16, pad=18)
plt.xlabel('Counts of Companies')
plt.show()

#Displaying the Bottom 10 Sectors by Company
(
    companies2
    .groupby('Sector')['Company']
    .count()
    .sort_values(ascending=True)
    .iloc[:-11]  
 ).plot(kind='barh') 
plt.title('Bottom 10 Sectors by Company', fontsize=14, pad=18)
plt.xlabel('Counts of Companies')
plt.show()


# 3. Who are the top 5 companies in terms of revenue? What is the difference from the previous company?

top5CompaniesByRevenue =(
    companies2
    [['Company','Revenue']]
    .sort_values(by='Revenue', ascending=False)
    .iloc[:5]
    .assign(diffFromPrevious = lambda x: x['Revenue'].diff(-1),
            pctChangeFromPrevious = lambda x: x['Revenue'].pct_change(-1).map(lambda x: '{:.2%}'.format(x) if pd.isna(x) == False else x))

)

print(top5CompaniesByRevenue)

top5CompaniesByRevenue.sort_values(by='Revenue', ascending=True).plot(kind='barh', x='Company', y='Revenue')
plt.title('Top 5 Companies by Revenue', fontsize=14, pad=18)
plt.show()

top5CompaniesByRevenue.sort_values(by='diffFromPrevious', ascending=True, na_position='first').plot(kind='barh', x='Company', y='diffFromPrevious')
plt.title('Top 5 Revenue Companies by Difference from Previous Value', fontsize=14, pad=18)
plt.show()

# 4.  What’s the average revenue across all companies in the dataset? What is the average revenue for the top 5 companies with the highest revenue? Get the median for the top 5 companies with the highest revenue. What is the pct_change from the average? What is the pct_change from the median?

avgRevenue = companies2['Revenue'].mean()
print(f'Avg. Revenue: {avgRevenue}')

top5RevenueByCompanyAvg=(
   companies2
    [['Company','Revenue']]
    .sort_values(by='Revenue', ascending=False)
    .iloc[:5]
    ['Revenue']
    .mean()
)
print(f'top5RevenueByCompanyAvg: {top5RevenueByCompanyAvg}')

top5RevenueByCompanyMedian = (
    companies2
    [['Company','Revenue']]
    .sort_values(by='Revenue', ascending=False)
    .iloc[:5]
    ['Revenue']
    .median()
)
print(f'top5RevenueByCompanyAvg: {top5RevenueByCompanyMedian}')

top5CompaniesByRevenue=(
    
    top5CompaniesByRevenue
    .sort_values(by='Revenue', ascending=False)
    .iloc[:5]
    .assign(diffFromAvg = lambda x: x['Revenue'] - avgRevenue,
            pctChangeFromAvg = lambda x: ((x['Revenue']/avgRevenue-1)).map(lambda x: '{:.2%}'.format(x/100) if pd.isna(x) == False else x),
            diffFromTop5Avg = lambda x: x['Revenue'] - top5RevenueByCompanyAvg,
            pctChangeFromTop5Avg = lambda x: ((x['Revenue']/top5RevenueByCompanyAvg-1)).map(lambda x: '{:.2%}'.format(x/100) if pd.isna(x) == False else x),
            diffFromTop5Median = lambda x: x['Revenue'] - top5RevenueByCompanyMedian,
            pctChangeFromTop5Median = lambda x: ((x['Revenue']/top5RevenueByCompanyMedian-1)).map(lambda x: '{:.2%}'.format(x/100) if pd.isna(x) == False else x),
            PctOfTop5Revenue = lambda x: (x['Revenue']/top5CompaniesByRevenue['Revenue'].sum()).map(lambda x: '{:.2%}'.format(x))
            )
)

#Displaying a table of the Top 5 companies by revenue with variance metrics
print(top5CompaniesByRevenue)

#Displaying the pattern between the top  5 companies revenue and variance from avg. revenue 
sns.scatterplot(top5CompaniesByRevenue, x='Revenue', y='pctChangeFromAvg', hue='Company')
plt.title('Revenue vs Variance from Average', fontsize=14, pad = 18)
plt.ylabel('Percent Change')
plt.show()

# 5. Who are the top 5 companies in terms of profit? Among the top five revenue-generating companies, which ones also rank within the top five in terms of profitability? 

# Top 5 Companies by Profit

top5CompaniesByProfit=(
    companies2
    [['Company','Profits']]
    .sort_values(by='Profits', ascending=False)
    .iloc[:5]
)
# displaying the top5 companies by profit
top5CompaniesByProfit.sort_values(by='Profits', ascending=True).plot(kind='barh', x='Company')
plt.xlabel('Profit')
plt.title('Top 5 Companies by Profit', fontsize=14, pad=18)
plt.show()

top5CompaniesByProfitWithVariance=(
    top5CompaniesByProfit
    .assign(inTop5Rev = lambda x: x['Company'].isin(top5CompaniesByRevenue['Company'].unique().tolist()),
            varianceFromPrevious = lambda x: x['Profits'].diff(-1),
            pctFromPrevious = lambda x: x['Profits'].pct_change(-1).map(lambda x: '{:.2%}'.format(x) if pd.isna(x) == False else x))
)

# displaying a table of the top 5 companies by profit then identifying which companies are also in the list of top 5 revenue with variance metrics
print(top5CompaniesByProfitWithVariance)


# 6. Which company has the lowest profit?  How many companies have negative profit?     

lowestProfit =(
    companies2
    [['Company','Profits']]
    .sort_values(by = 'Profits' , ascending =True)
    .iloc[0]
)

print(lowestProfit)

companiesWithNegativeProfit =(
    companies2
    [['Company','Profits']]
    .query('Profits < 0')
    .shape[0]
    
)

print(f'Companies with negative profit: {companiesWithNegativeProfit}')

# 7. Which state has the highest number of Fortune 1000 companies? 

# Top 3 States by Number of Fortune 1000 Companies

top3StatesByNumOfCountries =(
    companies2
    .groupby('State')['Company']
    .count()
    .sort_values(ascending=False)
    .iloc[:3]
)

print(top3StatesByNumOfCountries)

# 8. Which state has the highest combined revenue from all its Fortune 1000 companies?

# Top 3 States by Revenue
top3StatesByRevenue =(
    companies2
    .groupby('State')['Revenue']
    .sum()
    .sort_values(ascending=False)
    .iloc[:3]
)

print(top3StatesByRevenue)

# 9. Is there a correlation between the number of employees a company has and its revenue?

print(f'Correlation between Revenue & Employees: {companies2["Revenue"].corr(companies2["Employees"]).round(3)}')

sns.scatterplot(data=companies2, x='Employees', y='Revenue')
plt.xscale('linear')
plt.title('Correlation between Revenue & Employees', fontsize= 16, pad=18)
plt.show()

# 10. What percentage of the total revenue does each sector contribute to? 

#Top 10 Sectors by pct of revenue


top10SectorsByRevenue = (
    companies2
    [['Sector','Revenue']]
    .sort_values(by='Revenue', ascending=False)
    .assign(pctOfTotalRevenue = lambda x: (x['Revenue']/companies2['Revenue'].sum()).map(lambda x: '{:.2%}'.format(x)),
            pctOfTop10Revenue = lambda x: (x['Revenue']/companies2['Revenue']
                                                        .sort_values(ascending=False)
                                                        .head(10)
                                                        .sum())
                                                        .map(lambda x: '{:.2%}'.format(x)))
    .iloc[:10]
)

print(top10SectorsByRevenue)