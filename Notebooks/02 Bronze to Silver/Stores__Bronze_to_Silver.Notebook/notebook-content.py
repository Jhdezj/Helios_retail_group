# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b32334ad-4e4b-43f6-a678-1c6e3ad4897d",
# META       "default_lakehouse_name": "Silver_Lakehouse",
# META       "default_lakehouse_workspace_id": "d944fa16-752c-47a8-a344-74a788bbcc57",
# META       "known_lakehouses": [
# META         {
# META           "id": "cb724bdd-67d3-4a9f-a393-8bd9c7bd13f4"
# META         },
# META         {
# META           "id": "b32334ad-4e4b-43f6-a678-1c6e3ad4897d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Stores Bronze to Silver  
# Notebook that takes the Stores tables from the Bronze Lakehouse and takes them to Silver carrying out a standardization and cleaning process.

# CELL ********************

# Different ways to read a table:

# a) Read the table directly using pyspark and referencing the corresponding Lakehouse
stores_df = spark.read.table("Bronze_Lakehouse.dbo.Stores")

# b) Read using pyspark and the absolut path
#sales_df =spark.read.format('delta').load('abfss://Helios_Retail_Group@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Tables/dbo/sales01')

# C) Read using pyspark and using a relative path
#sales_df = spark.read.format('delta').load('Tables/dbo/sales01')

#display(stores_df.limit(3))
stores_df.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stores_df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform the data (TRANSFORM)  
# For educational purposes only, we will use pandas to do the transformations in this project. 

# CELL ********************

# Create a pandas dataframe
stores_df = stores_df.toPandas()

# Convert object to string, null into nullable 64, and null bool to nullable boolean. 
stores_df = stores_df.convert_dtypes()

# At this point we now have a pandas dataframe
stores_df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Rename Columns according to ER diagram

# CELL ********************

stores_df = stores_df.rename(columns={
"store_id": "id",
"store_name": "name",
"store_type": "type",
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stores_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save/Write to a table (LOAD)  
# We will use the *override* mode because we need to update the table every time

# CELL ********************

## # Conversion to spark Data Frame
spark_df = spark.createDataFrame(stores_df)
spark_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --delete from Silver_Lakehouse.dbo.Stores
# MAGIC drop table Silver_Lakehouse.dbo.Stores

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Save as delta table in the Bronze Lakehouse
# Note: in order to use the relative path, we need to set the Bronze Lakehouse as the Default lakehouse for this notebook
spark_df.write.format("delta").mode("overwrite").save(
    "abfss://Helios_Retail_Group@onelake.dfs.fabric.microsoft.com/"
    "Silver_Lakehouse.Lakehouse/Tables/dbo/Stores"
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
