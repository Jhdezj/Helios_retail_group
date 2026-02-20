# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b8e96fdf-09b9-4e44-908f-93d87a455cb0",
# META       "default_lakehouse_name": "Gold_Lakehouse",
# META       "default_lakehouse_workspace_id": "d944fa16-752c-47a8-a344-74a788bbcc57",
# META       "known_lakehouses": [
# META         {
# META           "id": "b8e96fdf-09b9-4e44-908f-93d87a455cb0"
# META         },
# META         {
# META           "id": "b32334ad-4e4b-43f6-a678-1c6e3ad4897d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Sales Bronze to Silver  
# Notebook that takes the Stores tables from the Silver Lakehouse and takes them to Gold carrying out buisiness transformations, modeling, and preparing for data reporting on Power BI.

# MARKDOWN ********************

# # Read data (Extract)

# CELL ********************

# Different ways to read a table:

# a) Read the table directly using pyspark and referencing the corresponding Lakehouse
sales_df = spark.read.table("Silver_Lakehouse.dbo.sales")

# b) Read using pyspark and the absolut path
#sales_df =spark.read.format('delta').load('abfss://Helios_Retail_Group@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Tables/dbo/sales01')

# C) Read using pyspark and using a relative path
#sales_df = spark.read.format('delta').load('Tables/dbo/sales01')

display(sales_df.limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform to pandas dataframe  
# (again, only for educational purposes -  in reality we coul use spark)

# CELL ********************

# Create a pandas dataframe
sales_df = sales_df.toPandas()

# Convert object to string, null into nullable 64, and null bool to nullable boolean. 
sales_df = sales_df.convert_dtypes()

# At this point we now have a pandas dataframe
display(sales_df.sample(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
