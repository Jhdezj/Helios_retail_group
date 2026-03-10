# Helios retail group
 Sales ETL Pipeline orchestration with Medallion Architecture on Fabric
 ## Overview 
This project demonstrates a complete ETL pipeline built on Microsoft Fabric, following the Medallion Architecture (Bronze → Silver → Gold) and integrated with a Semantic Model for analytics consumption in Power BI. The solution processes daily sales data, transforms it into analytics-ready tables, and powers business insights through an interactive dashboard. The pipeline  is schedule to tuns automatically every day. 

<img width="1537" height="758" alt="image" src="https://github.com/user-attachments/assets/a0c39586-309c-4293-954f-8b3a70b54248" />



## Business Scenario

Retail organizations need clean, structured, and analytics-ready data to track: sales performance, product trends, store efficiency, and time-based KPIs. This project simulates a retail analytics solution that transforms transactional data into a star-schema-ready model, optimized for reporting and business intelligence.

## Layers
The project has four layers integrated in four Microsoft Fabric Lakehouses. The **Raw layer** contains manually ingested data by an operational user or from any automated process. The **raw Layer** contains raw ingested data that represents the source-of-truth sotorage reachable for the data department but not by any external user. The **Silver layer** contains, cleaned and validated data, standardized formats, and deduplicated and enriched records. Finally, the **Gold layer** contains the Business-ready dimensional model.

## Semantic Model
The Gold layer is connected to a semantic model that defines a **Star schema** design which defines relationships (facts and dimensions), implements measures, enables time intelligence, and optimizes query performance. 


<img width="946" height="580" alt="image" src="https://github.com/user-attachments/assets/35f678c2-a32f-4ba7-8430-831c9b4d577a" />

## Pipeline
The process run trhough a pipeline built with Microsoft Fabric Pipelines that is programmed to run daily. It automates data ingestion, transformations among layers, Gold layer refresh, and semantic model update. 

<img width="794" height="518" alt="image" src="https://github.com/user-attachments/assets/5bf74649-fff6-4ef3-a597-2d8ff5529071" />

## Power Bi Dashboard
The KPIs implemented are sales by category, sales by store, average transaction value, sales by weekday vs weekend, year-over-year growth, and monthly sales trend.
<img width="1391" height="738" alt="image" src="https://github.com/user-attachments/assets/ac68ae1b-870b-4db3-a714-0782d8f878af" />


## How to re-integrate this project into fabric
- Create a Fabric Workspace
- Create a lakehouse structure
- Import the files from this repository
- Rebuild the pipeline
- Publish the Power BI Dashboard

## Future improvements
- Data quality monitoring layer
- Row-Level Security(RLS)
- Real-time streaming ingestion

## Tech Stack

- Microsoft Fabric
- Lakehouse Architecture
- Medallion Architecture
- Python Notebooks
- Power BI
- Data Pipelines
- Semantic Modeling




