# Azure & Snowflake End-To-End CRM Data Solution

![CRM Data Flow Diagram](https://github.com/ConatusForever/Python-Data-Projects/blob/main/Data%20Engineering/CRM/CRMDataFlowDiagram.png?raw=true)

# Project Overview
___
This is a End-To-End Azure & Snowflake analytics engineering project where I used Python, SnowSQL, SnowPark & Power BI. Multiple CRM files are read from local storage and pushed to Azure Data Lake Gen2. From there I used snowSQL to build a virtual data warehouse to stage and manage my data. SnowPark was used to transform and clean the data. Power BI was used as the visualization tool used for analytical consumption.

# ETL Framework Overview
___
**Extract:** Retrieve data files from Azure Blob Storage.
**Stage:** Upload the data files to Snowflake's internal or external staging area.
**Transform:** Use SnowPark to clean and transform the data.
**Load:** Load the transformed data into Snowflake tables.

