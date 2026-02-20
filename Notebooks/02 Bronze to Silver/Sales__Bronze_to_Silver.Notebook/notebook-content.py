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

# # Sales Bronze to Silver  
# Notebook that takes the Stores tables from the Bronze Lakehouse and takes them to Silver carrying out a standardization and cleaning process.

# MARKDOWN ********************

# ## Read data (EXTRACT)

# CELL ********************

# Different ways to read a table:

# a) Read the table directly using pyspark and referencing the corresponding Lakehouse
sales_df = spark.read.table("Bronze_Lakehouse.dbo.sales01")

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

# CELL ********************

sales_df.printSchema()

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

# CELL ********************

sales_df.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data type enforcement 
# * Standardize data types

# CELL ********************

#Data type enforcement
# OPTION A: Using pyspark (if the dataframe is a spark dataframe)
# from pyspark.sql.functions import col, to_timestamp, to_date
# df = df \
#     .withColumn("transaction_id", col("transaction_id").cast("string")) \
#     .withColumn("store_id", col("store_id").cast("string")) \
#     .withColumn("product_id", col("product_id").cast("string")) \
#     .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp"), "M/d/yyyy H:mm"))\
#     .withColumn("quantity", col("quantity").cast("int")) \
#     .withColumn("unit_price", col("unit_price").cast("decimal(18,2)")) \
#     .withColumn("total_amount", col("quantity") * col("unit_price"))

# OPTION B: Using spark sql (if the dataframe is a spark dataframe)
# using the sql flavor of spark
# (to-do just to explor, if you want)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Option B: use pandas

# Data type validation
expected_dtypes = {
    "transaction_id": "string",
    "store_id": "Int64",
    "transaction_timestamp": "datetime64[ns]",
    #"transaction_timestamp": "string",
    "product_id": "string",
    "category": "string",
    "quantity": "Int64",
    "unit_price": "Float64"
}

for col, expected in expected_dtypes.items():
    if str(sales_df[col].dtype) != expected:
        print(f"{col} is {sales_df[col].dtype}, expected {expected}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Data type casting
sales_df = sales_df.astype(expected_dtypes)
display(sales_df.sample(3))
print(sales_df.dtypes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.info()

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
sales_df.duplicated().any()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#See duplicate rows

sales_df[sales_df.duplicated()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop duplicates, by default keeps the first occurrence and drop later duplicates

sales_df = sales_df.drop_duplicates()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Duplicated transaction id's handling

# CELL ********************

# Detect duplicated id's
if sales_df["transaction_id"].duplicated().any():
    #raise ValueError("Duplicate transaction_id detected!")
    print(
        f"WARNING: Duplicate transaction_id detected!" 
        f"{sales_df['transaction_id'].duplicated().sum()} duplicate values."
    )

    print("Duplicate values: ")
    # See all duplicated id's
    print(sales_df[sales_df["transaction_id"].duplicated(keep=False)])

    print("Duplicated values by id:")
    # See which transaction Id's are duplicated and count them
    sales_df["transaction_id"].value_counts()[lambda x: x > 1]
else:
    print("No duplicate transaction id's")

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
sales_df.isnull().any()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check nulls per column
sales_df.isnull().sum()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# a) Drop rows with nulls
sales_df = sales_df.dropna() # Remove rows where ALL their values are nulls

# b) Fill with a constant
sales_df["category"] = sales_df["category"].fillna("Unknown")
# In this case, we DO NEED that if unit_price is empty, it remains like that
#sales_df["unit_price"] = sales_df["unit_price"].fillna(0.0) # not used

# c) Fill with mean/median
# NOT REALLY USEFUL IN THIS CASE, BECAUSE IT MAY BE MISLEADING
#sales_df["unit_price"] = sales_df["unit_price"].fillna(sales_df["unit_price"].mean())
#sales_df["quantity"] = sales_df["quantity"].fillna(sales_df["quantity"].median())

# d) Fill with forward/backward values
# NOT NEEDED HERE, because we don't know FOR SURE that was the date or timestamp
#sales_df["transaction_timestamp"] = sales_df["transaction_timestamp"].fillna(method="ffill")  # previous value
#sales_df["transaction_timestamp"] = sales_df["transaction_timestamp"].fillna(method="bfill")  # next value

# product and store id's will be treated in the next section

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Custom business rules 
# E.g., quantity and unit_price must be positive,etc. 

# CELL ********************

# Business Rules
# 
invalid_rows = sales_df[(sales_df["quantity"] <= 0) | (sales_df["unit_price"] <= 0)]

# Check for inconsistencies
if not invalid_rows.empty:
    print("There are invalid transactions.")    
    # Invalid Rows
    num_invalid = ((sales_df["quantity"] <= 0) | (sales_df["unit_price"] <= 0)).sum()
    print(f"Number of invalid transactions: {num_invalid}")
    print(f"Invalid transactions: \n{invalid_rows}")
else: 
    print("No invalid transactions found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter only valid transactions
df_valid = sales_df[(sales_df["quantity"] > 0) & (sales_df["unit_price"] > 0)]
display(df_valid.sample(3))

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
spark_df = spark.createDataFrame(df_valid)
display(spark_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --delete from Silver_Lakehouse.dbo.sales01_silver
# MAGIC drop table Silver_Lakehouse.dbo.sales01_silver

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
    "Silver_Lakehouse.Lakehouse/Tables/dbo/Sales"
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
