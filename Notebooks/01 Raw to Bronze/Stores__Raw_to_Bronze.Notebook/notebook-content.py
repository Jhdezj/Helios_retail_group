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

# # Stores Raw to Bronze
# Notebook that creates the operations stores table and load it into the Bronze Lakehouse as a table in Delta Format

# CELL ********************

# Manual creation of Current operational stores under Helios Retail Group.
import pandas as pd 
data = {
    "store_id": [1,2,3,4,5],
    "store_name":["Helios Downtown Flagship", "Helios Lakeside Plaza", "Helios Riverside Outlet", "Helios Mountain View", "Helios Coastal Market"],
    "location": ["Austin, TX", "Madison, WI", "Savannah, GA", "Denver, CO", "San Diego, CA"],
    "store_type":["Flagship Urban Retail", "Shopping Mall Anchor", "Outlet Store", "Lifestyle Concept Store", "Premium Coastal Boutique"],
    "opening_date": ["2018-03-15", "2019-07-22", "2020-11-05", "2021-09-30", "2017-05-18", ],
    "floor_area": [2800, 1950, 1400, 2200,1750],
    "regional_manager":["Melissa Grant", "Carlos Mendoza", "Danielle Brooks", "Nathan Chen", "Aisha Rahman"]
} 
df = pd.DataFrame(data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df

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

# Save as delta table in the Bronze Lakehouse
# Note: in order to use the relative path, we need to set the Bronze Lakehouse as the Default lakehouse for this notebook
spark_df.write.format("delta").mode("overwrite").save("Tables/dbo/Stores")
# same as spark_df.write.format("delta").mode("overwrite").saveAsTable("dbo.Stores")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
