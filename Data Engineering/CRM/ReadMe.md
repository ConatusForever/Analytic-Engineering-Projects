# Azure & Snowflake End-To-End CRM Data Solution
![CRM Data Flow Diagram](https://github.com/ConatusForever/Python-Data-Projects/blob/main/Data%20Engineering/CRM/azure-data-warehouse-vs-snowflake.jpg?raw=true)

# Business Value Proposition
This comprehensive end-to-end Customer Relationship Management (CRM) data solution is aimed at transforming the way my client understands and interact with their customers.
Through the implementation of this CRM data solution, I aim to:
* **Enhance Customer Insights**: Provide actionable insights into customer purchasing behaviors.
* **Identify Product Sales Opportunities**: Uncover new opportunities to boost product sales.
* **Optimize Sales Team Performance**: Identify top-performing and underperforming sales teams to improve overall performance.

# Analytic Engineering Skills
To develop this solution, I utilized a variety of skills including:
* SQL
* SnowPark
* Snowflake Virtual Data Warehouse
* Azure Blob Storage
* Schema Design
* ETL Processes
* Data Cleaning
* Data Transformation
* Data Modeling
* Statistics
* Data Analysis

# Data Engineering
To create the pipeline and data modeling, I used Python, SQL, and SnowPark. Multiple CRM files were read from my local file storage and pushed to Azure Data Lake Gen2. From there, I used SQL to build a virtual data warehouse to stage and manage my data. SnowPark was used to transform and clean the data. Power BI was used as the visualize the entity relationship diagram (ERD) and provide analytical insights.

### Extract, Transform, Load
![image](https://github.com/ConatusForever/Python-Data-Projects/blob/main/Data%20Engineering/CRM/CRMDataFlowDiagram.png?raw=true)
**Steps**:
1. Read csv files from local file storage
2. Pushed the raw files to Azure Data Lake Gen2
3. Use Snowflake Service Principal to load files in into my snowflake staging area
4. Transformed the data with snowPark & updated my data warehouse tables

### Data Warehouse
Choosing Snowflake as the data solution for this CRM project was ideal due to its robust feature set, including query optimization, auto-scaling, and concurrency handling. Snowflake's architecture separates compute and storage, allowing for independent and elastic scaling This helps to ensure efficient resource usage and cost management. Snowflake's query optimization capabilities provide high-speed performance by utilizing advanced indexing and caching techniques. Auto-scaling automatically adjusts resources based on workload demands, maintaining consistent performance without manual intervention. 
![image](https://github.com/ConatusForever/Python-Data-Projects/blob/main/Data%20Engineering/CRM/CRMDB.png?raw=true)


### Data Modeling
![image](https://github.com/ConatusForever/Python-Data-Projects/blob/main/Data%20Engineering/CRM/CRMDataModel.png?raw=true)

# Dashboard Design and Visualization
Development of the dashboard is currently underway.

# Conclusion
currently underway.

