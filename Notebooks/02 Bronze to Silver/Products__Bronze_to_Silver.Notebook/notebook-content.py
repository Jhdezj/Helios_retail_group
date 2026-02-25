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

# # Products Bronze to Silver  
# Notebook that takes the Products tables from the Bronze Lakehouse and takes them to Silver carrying out a standardization and cleaning process.

# MARKDOWN ********************

# ## Read data (EXTRACT)

# CELL ********************

# Different ways to read a table:

# a) Read the table directly using pyspark and referencing the corresponding Lakehouse
products_df = spark.read.table("Bronze_Lakehouse.dbo.Products")

# b) Read using pyspark and the absolut path
#products_df =spark.read.format('delta').load('abfss://Helios_Retail_Group@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Tables/dbo/Products')

# C) Read using pyspark and using a relative path
#products_df = spark.read.format('delta').load('Tables/dbo/Products')

display(products_df.limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform the data (TRANSFORM)  
# For educational purposes only, we will use pandas to do the transformations in this project. 

# MARKDOWN ********************

# ### Transform to pandas dataframe  
# (again, only for educational purposes -  in reality we coul use spark)

# CELL ********************

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

products_df.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Column renaming

# CELL ********************

column_mapping = {
    "ProductID": "id", 
    "ProductName": "name", 
    "Category": "category", 
    "Brand": "brand", 
    "UnitPrice": "unit_price", 
    "LaunchYear": "launch_year"
}
column_mapping

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_df.rename(columns = column_mapping, inplace=True)
products_df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data type enforcement 
# * Standardize data types

# CELL ********************

# Option B: use pandas

# Data type validation
expected_dtypes = {
    "id":           "Int64",
    "name":         "string",
    "category":     "string",
    "brand":        "string",
    "unit_price":   "Float64",
    "launch_year":  "Int64"
}

for col, expected in expected_dtypes.items():
    if str(products_df[col].dtype) != expected:
        print(f"{col} is {products_df[col].dtype}, expected {expected}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Data type casting
products_df = products_df.astype(expected_dtypes)
display(products_df.sample(3))
print(products_df.dtypes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 

# MARKDOWN ********************

# ### Duplicated rows handling 
# Check and handle duplicates

# CELL ********************

#Detect Duplicate rows
products_df.duplicated().any()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#See duplicate rows
products_df[products_df.duplicated()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop duplicates, by default keeps the first occurrence and drop later duplicates
products_df = products_df.drop_duplicates()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Duplicated product id's handling

# CELL ********************

# Detect duplicated id's
if products_df["id"].duplicated().any():
    #raise ValueError("Duplicate id detected!")
    print(
        f"WARNING: Duplicate id detected!" 
        f"{products_df['id'].duplicated().sum()} duplicate values."
    )

    print("Duplicate values: ")
    # See all duplicated id's
    print(products_df[products_df["id"].duplicated(keep=False)])

    print("Duplicated values by id:")
    # See which transaction Id's are duplicated and count them
    products_df["id"].value_counts()[lambda x: x > 1]
else:
    print("No duplicate id's")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Null handling  
# Check for null values and remove or fill in

# CELL ********************

# Quick check if any nulls exist
products_df.isnull().any()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check nulls per column
products_df.isnull().sum()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# a) Drop rows with nulls
products_df = products_df.dropna() # Remove rows where ALL their values are nulls

# b) name, category, brand
products_df["name"] = products_df["name"].fillna("Unknown")
products_df["category"] = products_df["category"].fillna("Unknown")
products_df["brand"] = products_df["brand"].fillna("Unknown")

# In this case, we DO NEED that if unit_price is empty, it remains like that
#products_df["unit_price"] = products_df["unit_price"].fillna(0.0) # not used

# launch year, keep as is

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Custom business rules 
# E.g., quantity and unit_price must be positive,etc.  
# In this case, the product id comes as integer, but in the sales table it comes as P##.  
# We will modify the sales table to map the values.

# MARKDOWN ********************

# ## Save/Write to a table (LOAD)  
# We will use the *append* mode because we need to keep the data from other stores and other dates. 

# CELL ********************

## # Conversion to spark Data Frame
spark_df = spark.createDataFrame(products_df)
display(spark_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --delete from Silver_Lakehouse.dbo.sales01_silver
# MAGIC drop table Silver_Lakehouse.dbo.Products

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
spark_df.write.format("delta").mode("append").save(
    "abfss://Helios_Retail_Group@onelake.dfs.fabric.microsoft.com/"
    "Silver_Lakehouse.Lakehouse/Tables/dbo/Products"
    )
# Another alternative:
# Something like this (buscar el formato exacto, que es "semi-largo")
# spark_df.write.format("delta").mode("append")\
#      .save("Silver_Lakehouse.Tables.dbo.Products")

# same as 
#spark_df.write.format("delta").mode("overwrite").saveAsTable("dbo.Products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
