-- Created using VS code Snowflake Extention
-- Database, Schema, Data Warehouse creation
create database if not exists crm
 comment ='Database for loading Azure data';

create schema if not exists crm.dbo
    comment ='Schema for tables loaded from Azure';

create warehouse if not exists dw_crm
with warehouse_size= 'xsmall';

--Table Creation 

create or replace table crm.dbo.customerAccounts(
    customerAccounts varchar(50) ,
    sector varchar(50),
    yearEstablished Number(4,0),
    revenue decimal(15,2),
    employees number(6,0),
    officeLocation varchar(50),
    subsidiaryOf varchar(50)
    
);

create table if not exists crm.dbo.products (
    product varchar(20),
    series varchar(10),
    salesPrice decimal(9,2)
    
);


create table if not exists crm.dbo.salesPipeline(
    opportunityId varchar(25),
    salesAgent varchar(50),
    product varchar(20),
    account varchar(50),
    dealStage varchar(25),
    engageDate date,
    closeDate date,
    closeValue number(9,2)
);

create table if not exists crm.dbo.salesTeams
(
    salesAgent varchar(50),
    manager varchar(50),
    regionalOffice varchar(50)
);


-- Creating Storage Integration

create or replace storage integration analyticEngineeringIntegration
    type = external_stage
    storage_provider = 'azure' 
    azure_tenant_id = 'a8ae74cb-9480-4769-b8e3-a20f0c7f3659'
    enabled = true
    storage_allowed_locations= ('azure://analyticsengdatalake.blob.core.windows.net/analytics-eng/');

-- using desc storage to get url to create azure role for snowflake application
-- desc storage integration analyticEngineeringIntegration;

grant create stage on schema dbo to accountadmin;
grant usage on integration analyticEngineeringIntegration to accountadmin;

 -- Creating Stage for data
use schema crm.dbo;
create or replace stage azureDataStage
storage_integration = analyticEngineeringIntegration
url = 'azure://analyticsengdatalake.blob.core.windows.net/analytics-eng/'
file_format = (type = csv);


-- Load Tables with Data

copy into crm.dbo.customeraccounts
from @crm.dbo.azureDataStage
files = ('accounts.csv')
file_format = (type = 'CSV' skip_header=1);

copy into crm.dbo.products
from @crm.dbo.azureDataStage
files = ('products.csv')
file_format = (type = 'CSV' skip_header=1);

copy into crm.dbo.salespipeline
from @crm.dbo.azureDataStage
files = ('sales_pipeline.csv')
file_format = (type = 'CSV' skip_header=1);

copy into crm.dbo.salesteams
from @crm.dbo.azureDataStage
files = ('sales_teams.csv')
file_format = (type = 'CSV' skip_header=1);
