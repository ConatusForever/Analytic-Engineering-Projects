
/*

Overview:

This script demonstrates the workflow for loading data from local files into Snowflake tables 
using the SnowSQL CLI. The process involves the following steps:

1. Table Creation: Empty tables were created for the candy business datasets.

2. Stage Creation: A Snowflake stage was created to serve as a temporary storage area for loading data.

3. Stage Loading: Local CSV files were uploaded to the Snowflake stage using the SnowSQL CLI.

4. Data Copying: The files from the stage were copied into the corresponding tables.

*/


/*----------------------------------------------------------------------------------------------
Table Creation
----------------------------------------------------------------------------------------------*/

-- Create Factories Table

create or replace table candy_factories(
	factory string,
	latitude float,
	longitude float
);

-- Create Targets Table

create or replace table candy_targets(
	division string,
	target int
);

-- Create Products Table

create or replace table candy_products(
	division string,
	product_name string,
	factory string,
	product_id string,
	unit_price float,
	unit_cost float
);

--Create Sales Table

create or replace table candy_sales(
	row_id integer,
	order_id string,
	order_date date,
	ship_date date,
	ship_mode string,
	customer_id integer,
	country string,
	city string,
	state string,
	postal_code string,
	division string,
	region string,
	product_id string,
	product_name string,
	sales float,
	units float,
	gross_profit float,
	cost float
);


/*---------------------------------------------------------------------------------------------
Stage Creation & File Loading to Stage
---------------------------------------------------------------------------------------------*/

create stage candy_business
file_format = (type = CSV);

put file://C:\Users\HakeemLawrence\Documents\Datasets\US_Candy_Distributor\Tables\*.csv @candy_business auto_compress=true;

/*---------------------------------------------------------------------------------------------
Load Tables
---------------------------------------------------------------------------------------------*/

-- Load Targets Table

copy into CANDY_TARGETS
from @RAW.candy_business/Candy_Targets.csv.gz
file_format =(
 type = 'CSV'
 compression = 'GZIP'
 skip_header =1
 );

-- Load Sales Table

copy into CANDY_SALES
from @RAW.candy_business/Candy_Sales.csv.gz
file_format =(
 type = 'CSV'
 compression = 'GZIP'
 skip_header =1
 );

-- Load Products Table

copy into CANDY_PRODUCTS
from @RAW.candy_business/Candy_Products.csv.gz
file_format =(
 type = 'CSV'
 compression = 'GZIP'
 skip_header =1
 );

-- Load Factories Table

copy into CANDY_FACTORIES
from @RAW.candy_business/Candy_Factories.csv.gz
file_format =(
 type = 'CSV'
 compression = 'GZIP'
 skip_header =1
 );
