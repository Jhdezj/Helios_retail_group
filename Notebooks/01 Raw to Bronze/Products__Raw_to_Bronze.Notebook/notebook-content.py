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

# # Products Raw to Bronze
# Notebook that takes the xlsx files from the Raw Lakehouse and load them as they are into the Bronze Lakehouse as tables in Delta Format (no cleaning nor standardization processing!)

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data into pandas DataFrame from Raw Lakehouse"
df = pd.read_excel("abfss://Helios_Retail_Group@onelake.dfs.fabric.microsoft.com/Raw_Lakehouse.Lakehouse/Files/xlsx/Helios Products Catalog.xlsx")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Conversion to spark Data Frame
spark_df = spark.createDataFrame(df)
display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save as delta table
spark_df.write.format("delta").mode("overwrite").save("Tables/dbo/Products")
# same as spark_df.write.format("delta").mode("overwrite").saveAsTable("dbo.Stores")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
