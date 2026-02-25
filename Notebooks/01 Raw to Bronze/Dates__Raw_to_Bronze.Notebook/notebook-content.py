# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cb724bdd-67d3-4a9f-a393-8bd9c7bd13f4",
# META       "default_lakehouse_name": "Bronze_Lakehouse",
# META       "default_lakehouse_workspace_id": "d944fa16-752c-47a8-a344-74a788bbcc57",
# META       "known_lakehouses": [
# META         {
# META           "id": "b32334ad-4e4b-43f6-a678-1c6e3ad4897d"
# META         },
# META         {
# META           "id": "d2f2d162-599c-42ef-a1cf-68b5aca2da35"
# META         },
# META         {
# META           "id": "cb724bdd-67d3-4a9f-a393-8bd9c7bd13f4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Dates Row to Bronze  
# Notebook that creates the Dates dimension table

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calendar table 

# Create date range
start_date = "2026-01-01"
end_date = "2030-12-31"
dates = pd.date_range(start=start_date, end=end_date, freq='D')


# Build dataframe
df = pd.DataFrame({'full_date': dates})

# Date key in format YYYYMMDD (integer)
df['date_key'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)

# Date attributes
df['year'] = df['full_date'].dt.year
df['quarter'] = df['full_date'].dt.quarter
df['month'] = df['full_date'].dt.month
df['day'] = df['full_date'].dt.day

# Monday=0, Sunday=6 (standard pandas behavior)
df['day_of_week'] = df['full_date'].dt.weekday

df['weekday_name'] = df['full_date'].dt.day_name()

# Weekend flag (Saturday=5, Sunday=6)
df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

# Reorder columns to match your Dim_Date structure
df = df[
    [
        'date_key',
        'full_date',
        'year',
        'quarter',
        'month',
        'day',
        'day_of_week',
        'weekday_name',
        'is_weekend'
    ]
]
df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save/Write to a table (LOAD)  
# We will use the *overwrite* mode because we need to create a clean table. 

# CELL ********************

## # Conversion to spark Data Frame
spark_df = spark.createDataFrame(df)
display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --delete from Silver_Lakehouse.dbo.Sales
# MAGIC drop table Silver_Lakehouse.dbo.Sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Save as delta table in the Silver Lakehouse
# Note: in order to use the relative path, we need to set the Silver Lakehouse as the Default lakehouse for this notebook
spark_df.write.format("delta").mode("overwrite").save(
    "abfss://Helios_Retail_Group@onelake.dfs.fabric.microsoft.com/"
    "Bronze_Lakehouse.Lakehouse/Tables/dbo/Dates"
    )
# Another alternative:
# Something like this (buscar el formato exacto, que es "semi-largo")
# spark_df.write.format("delta").mode("append")\
#      .save("Silver_Lakehouse.Tables.dbo.Sales")

# same as 
#spark_df.write.format("delta").mode("overwrite").saveAsTable("dbo.Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
