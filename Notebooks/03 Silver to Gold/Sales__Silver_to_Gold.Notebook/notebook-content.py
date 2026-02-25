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

# # Sales Silver to Gold 
# Notebook that takes the Stores tables from the Silver Lakehouse and takes them to Gold carrying out business transformations, modeling, and preparing for data reporting on Power BI.

# MARKDOWN ********************

# **Analytic questions to be answered:**
# 
# - What are daily sales per store?
# - Revenue by product/category?
# - Average basket size?
# - Top-performing stores?
# - Time-based trends(hourly/daily/weekly/monthly)?

# MARKDOWN ********************

# ## Read data (Extract)

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

# # Transform

# MARKDOWN ********************

# ### Convert table to a pandas dataframe  
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

# MARKDOWN ********************

# ## Derived Business Metrics (Fact table measures)

# CELL ********************

sales_df["total_amount"] = sales_df["quantity"] * sales_df["unit_price"]
display(sales_df.sample(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Referential Integrity

# MARKDOWN ********************

# ### Stores

# CELL ********************

# Read 
stores_df = spark.read.table("Gold_Lakehouse.dbo.Stores")

# Create a pandas dataframe
stores_df = stores_df.toPandas()

# Convert object to string, null into nullable 64, and null bool to nullable boolean. 
stores_df = stores_df.convert_dtypes()

# At this point we now have a pandas dataframe
display(stores_df.sample(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# store_id referential integrity
invalid_stores = sales_df[~sales_df["store_id"].isin(stores_df["id"])]

if invalid_stores.empty:
    print("Referential integrity valid for store_id")
else:
    print("Invalid store_id's found")
    print(invalid_stores)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  Remove the rows with invalid stores_id:
sales_df = sales_df[sales_df["store_id"].isin(stores_df["id"])]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Products 

# CELL ********************

# Read 
products_df = spark.read.table("Gold_Lakehouse.dbo.Products")

# Create a pandas dataframe
products_df = products_df.toPandas()

# Convert object to string, null into nullable 64, and null bool to nullable boolean. 
products_df = products_df.convert_dtypes()

# At this point we now have a pandas dataframe
display(products_df.sample(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# product_id referential integrity
invalid_product = sales_df[~sales_df["product_id"].isin(products_df["id"])]

if invalid_product.empty:
    print("Referential integrity valid for product_id")
else:
    print("Invalid product_id found")
    print(invalid_product)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  Remove the rows with invalid product_id:
sales_df = sales_df[sales_df["product_id"].isin(products_df["id"])]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save/Write to a table (LOAD)  
# We will use the *append* mode because we need to keep the data from other stores and other dates. 

# CELL ********************

## # Conversion to spark Data Frame
spark_df = spark.createDataFrame(sales_df)
display(spark_df.limit(3))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save as delta table in the Gold Lakehouse
# Note: in order to use the relative path, we need to set the Gold Lakehouse as the Default lakehouse for this notebook
spark_df.write.format("delta").mode("append").save(
    "abfss://Helios_Retail_Group@onelake.dfs.fabric.microsoft.com/"
    "Gold_Lakehouse.Lakehouse/Tables/dbo/Sales"
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
