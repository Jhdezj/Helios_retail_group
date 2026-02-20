# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d2f2d162-599c-42ef-a1cf-68b5aca2da35",
# META       "default_lakehouse_name": "Raw_Lakehouse",
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

# # Stores Raw to Bronze  
# Notebook that takes the csv files from the Raw Lakehouse and converts them as they are and puts them into the Bronze Lakehouse (no cleaning nor standardization processing!)

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data into pandas DataFrame from "/lakehouse/default/Files/csv/store_01_2026-02-01.csv"
df = pd.read_csv("/lakehouse/default/Files/csv/store_01_2026-02-01.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df = spark.createDataFrame(df)
display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assuming 'spark' session is available
# Crea la tabla DIRECTAMENTE en el DEFAULT lakehouse
spark_df.write.format("delta").mode("overwrite").saveAsTable("table1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assuming 'spark' session is available
# Crea la tabla en cualquier otro lakehouse especificando la ruta:
lakehouse_name = "Bronze_Lakehouse"
schema = "dbo"
workspace_name = "Helios_Retail_Group"
table_name = "sales01"

# Si workspace_name NO tiene espacios, usar esto:
spark_df.write.format("delta").mode("append")\
    .save(f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/{schema}/{table_name}")

# Si workspace_name SÍ tiene espacios, debo usar los IDs (es otra ruta)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data into pandas DataFrame from "/lakehouse/default/Files/csv/store_01_2026-02-01.csv"
df2 = pd.read_csv("/lakehouse/default/Files/csv/store_02_2026-02-01.csv")
display(df2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
