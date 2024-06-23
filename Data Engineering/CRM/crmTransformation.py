from  snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import StructField, StructType, IntegerType, StringType, VariantType
from snowflake.snowpark.window import Window

session = get_active_session()

# Update Tables for Key Columns

# If Id column exists, drop it so that we can recreate it

def dropColumnIfExists(tableName, columnName):
    """
    Drops the specified column from the specified table if it exists.

    Args:
        tableName (str): The name of the table from which the column should be dropped.
        columnName (str): The name of the column to be dropped.

    Returns:
        None
    """
    try:
        session.sql(f'ALTER TABLE {tableName} DROP COLUMN IF EXISTS {columnName}').collect()
        print(f'Column {columnName} dropped from table {tableName}.')
    except Exception as e:
        print(f'Error dropping column {columnName} from table {tableName}: {e}')


# Transforming the Customer Accounts Table

dropColumnIfExists('customerAccounts','customerId')
# addColumnIfNotExists('customerAccounts','customerId','string')
customerAccounts = 'customeraccounts'
customerAccountsDf = session.table(customerAccounts)

newRowSchema = StructType([
    StructField('customerName', StringType(), nullable=False),
    StructField('customerId', StringType(), nullable=False)
])

tbdCustomerRow = session.create_dataframe([('TBD','5999')], schema=newRowSchema)

customerIds = (
    customerAccountsDf
    .select('CUSTOMERACCOUNTS')
    .distinct()
    .with_column('customerId', row_number().over(Window.order_by('CUSTOMERACCOUNTS'))+5000)
    .with_column('customerId', col('CUSTOMERID').cast(StringType()))
    .withColumnRenamed('customeraccounts','customerName')
    .union(tbdCustomerRow)
)

customerAccountsJoined=(
    customerAccountsDf
    .na.fill({'SUBSIDIARYOF':'Not Specified'})
    .join(customerIds,customerAccountsDf["CUSTOMERACCOUNTS"] == customerIds["customerName"],'left_outer')
    .drop(customerIds.col('customerName'))
)
customerAccountsReordered = customerAccountsJoined.select(['customerId'] + [col for col in customerAccountsJoined.columns if col != 'CUSTOMERID'])
customerAccountsReordered.write.mode('overwrite').save_as_table('customerAccounts')
session.sql('''
alter table customerAccounts add constraint pkCustomerAccounts primary key (customerId);
''')
print('CustomerAccounts Primary Key Column Added')

customerAccountsReordered.show(5)

# Transforming the Products Table

dropColumnIfExists('products','productId')
products = 'products'
productsDf = session.table(products)

productIds =(
    productsDf
    .select('product')
    .distinct()
    .with_column('productId', row_number().over(Window.order_by('PRODUCT'))+4000)
    .with_column('productId', col('PRODUCTID').cast(StringType()))
    .withColumnRenamed('product','productName')
)

productsJoined =(
                    productsDf
                    .join(productIds, productsDf['product'] == productIds['productName'], 'left_outer')
                    .drop(productIds.col('productName'))
                )
productsReordered= productsJoined.select(['productId'] + [col for col in productsJoined.columns if col != 'PRODUCTID'])
productsReordered.write.mode('overwrite').save_as_table('products')
session.sql('''
alter table products add constraint pkProducts primary key (productId);
''')
print('Products Primary Key Column Added')
productsReordered.show(5)

dropColumnIfExists('salesTeams','salesAgentId')
salesteams = 'salesteams'
salesTeamsDf = session.table(salesteams)

# Update Sales Teams Table

salesAgentids= (
    salesTeamsDf
    .select('salesagent')
    .distinct()
    .withColumn('salesAgentID', row_number().over(Window.order_by('SALESAGENT'))+3000)
    .withColumn('salesAgentId', col('salesAgentId').cast(StringType()))
    .withColumnRenamed('salesAgent','salesAgentName')
)

salesTeamsJoined=(
    salesTeamsDf
    .join(salesAgentids,salesTeamsDf['salesAgent'] == salesAgentids['salesAgentName'], 'left_outer')
    .drop(salesAgentids.col('salesAgentName'))
)
salesTeamsReordered = salesTeamsJoined.select(['salesAgentId'] + [col for col in salesTeamsJoined.columns if col != 'SALESAGENTID'])
salesTeamsReordered.write.mode('overwrite').save_as_table('salesTeams')
session.sql('''
alter table salesTeams add constraint pkSalesTeams primary key (salesAgentId);
''')
print('SalesTeam Primary Key Column Added')
salesTeamsReordered.show(5)


# Update Sales Pipeline Table
dropColumnIfExists('salesPipeline','salesAgentId')
dropColumnIfExists('salesPipeline','productId')
dropColumnIfExists('salesPipeline','customerId')
salespipeline = 'salespipeline'
salesPipelineDf = session.table(salespipeline)

salesPipelineJoined= (
    salesPipelineDf
    .na
    .fill({'account':'TBD'})
    .with_column('product', when(col('product') =='GTXPro','GTX Pro').otherwise(col('product')))
    .join(salesAgentids, salesPipelineDf['salesAgent'] == salesAgentids['salesAgentName'], 'left_outer')
    .join(productIds, salesPipelineDf['product'] == productIds['productName'], 'left')
    .join(customerIds, salesPipelineDf['account'] == customerIds['customerName'], 'left_outer')
    .drop(salesAgentids['salesAgentName'], productIds['productName'], customerIds['customerName'])
)
salesPipelineReordered= salesPipelineJoined.select('opportunityid','salesagentid','salesagent','customerid','account',
                                                   'productid','product','dealstage','engagedate','closedate','closevalue')

# Checking for referential integrity before creating salesPipeline view
invalidSalesAgents = salesPipelineReordered.filter(~col('salesAgentId').isin([row['SALESAGENTID'] for row in salesAgentids.collect()]))
invalidProducts = salesPipelineReordered.filter(~col('productId').isin([row['PRODUCTID'] for row in productIds.collect()]))
invalidCustomers = salesPipelineReordered.filter(~col('customerId').isin([row['CUSTOMERID'] for row in customerIds.collect()]))

if invalidSalesAgents.count() > 0 or invalidProducts.count() > 0 or invalidCustomers.count() > 0:
    raise ValueError("Referential integrity check failed. There are invalid foreign key references in the sales pipeline data.")

salesPipelineReordered.write.mode('overwrite').save_as_table('salesPipeline')
session.sql('''
alter table salesPipeline add constraint pkSalesPipeline primary key (opportunityId);
''')
session.sql(
    '''
    alter table SalesPipeline add constraint fkSalesPipelineSalesAgents foreign key (salesagentid) references SalesTeams(salesAgentId); 
    '''
)

session.sql(
    '''
    alter table SalesPipeline add constraint fkSalesPipelineProducts foreign key (productId) references Products(productId); 
    '''
)

session.sql(
    '''
    alter table SalesPipeline add constraint fkSalesPipelineCustomer foreign key (customerId) references CustomerAccounts(customerId); 
    '''
)
print('Sales Pipeline Foreign Key Columns Added')
salesPipelineReordered.show(5)

# Checking for Duplicate Rows and Missing Values
dfNamesList = [customerAccounts, products, salesteams, salespipeline]
dfList = [customerAccountsReordered, productsReordered, salesTeamsReordered, salesPipelineReordered]
dfCombo = dict(zip(dfNamesList,dfList))

for dfName, df  in dfCombo.items(): 
    dups =(
            df
            .group_by(*df.schema.names)
            .count()
            .filter(col('count') > 1)
            .select(sum('count').alias('totalDuplicateCount'))
    )
    
    totalDups = dups.collect()[0]['TOTALDUPLICATECOUNT']
    
    missingData = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.schema.names 
    ])
    
    print(f'Total Duplicate for {dfName}: {totalDups}')
    missingData.show()