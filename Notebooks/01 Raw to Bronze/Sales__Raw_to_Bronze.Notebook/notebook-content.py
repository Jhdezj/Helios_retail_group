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

# # Sales Raw to Bronze  
# Notebook that takes the csv files from the Raw Lakehouse and converts them as they are and puts them into the Bronze Lakehouse (no cleaning nor standardization processing!)

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read data (EXTRACT)

# CELL ********************

 
from notebookutils import mssparkutils


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Read files from the raw lakehouse 

# CELL ********************

raw_files = mssparkutils.fs.ls("Files/csv/")
raw_file_names = [f.name for f in raw_files if f.name.endswith(".csv")]

print(raw_file_names)


bronze_path = "abfss://Helios_Retail_Group@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Files/Processed"

bronze_files = mssparkutils.fs.ls(bronze_path)
bronze_file_names = [f.name for f in bronze_files if f.name.endswith(".csv")]

print(bronze_file_names)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Obtain unprocessed files

# CELL ********************

raw_set = set(raw_file_names)
bronze_set = set(bronze_file_names)

unprocessed_files = raw_set - bronze_set

print("Missing in Bronze:", unprocessed_files)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform and Load  
# Process the files that are missing in the Bronze lakehouse into a sales table

# CELL ********************

import pandas as pd
import os

base_path = "/lakehouse/default/Files/csv/"

#file_names = [f for f in os.listdir(base_path) if f.endswith(".csv")]

dfs = []
for file in unprocessed_files:
    full_path = os.path.join(base_path, file)
    df = pd.read_csv(full_path)
    dfs.append(df)

if len(dfs)>0:
    Sales = pd.concat(dfs, ignore_index=True)
    spark_df = spark.createDataFrame(Sales)
    display(spark_df)

    # WRITE the spark dataframe (append)
    # Assuming 'spark' session is available
    # Crea la tabla en cualquier otro lakehouse especificando la ruta:
    lakehouse_name = "Bronze_Lakehouse"
    schema = "dbo"
    workspace_name = "Helios_Retail_Group"
    table_name = "Sales"

    # Si workspace_name NO tiene espacios, usar esto:
    spark_df.write.format("delta").mode("append")\
        .save(f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/{schema}/{table_name}")

    # Si workspace_name SÍ tiene espacios, debo usar los IDs (es otra ruta)

    # MOVE the files to processed
    # Technical requirement: once the files have been processed, 
    # move them to the Bronze layer (Bronze_Lakehouse)
    for file in unprocessed_files:
        source_path = "file:" + os.path.join(base_path, file)
        destination_path = os.path.join(bronze_path, file)
        print(f"Moving {file}...")
        mssparkutils.fs.mv(source_path, destination_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
