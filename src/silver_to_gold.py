from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

spark = SparkSession.builder.appName('new_app').config('spark.executor.memory', '4g').getOrCreate()

silver_path = "hdfs://localhost:9005/data_lake/silver/"
gold_path = "hdfs://localhost:9005/data_lake/gold/"
last_processed_file = "/home/naveen/spark_workout/project/last_processed.txt"


if os.path.exists(last_processed_file):
    with open(last_processed_file, "r") as f:
        last_processed = f.read().strip()
else:
    last_processed = "1900-01-01"

df = spark.read.option("recursiveFileLookup", "true").parquet(silver_path)    
df = df.withColumn('partition_date', to_date(concat_ws("-", col('Year'), col('Month'), col('Day_Month'))))
df_new = df.filter(col('partition_date') > last_processed)

if df_new.isEmpty():
    print("No new data to process")
    spark.stop()
    exit()


df_new.write.mode('overwrite').saveAsTable('sil_table')

def dimension_table_insert(dim_path, dim_df, id_col):
    try:
        df_existing = spark.read.parquet(dim_path)
        df_final = df_existing.unionByName(dim_df).dropDuplicates([id_col])
    except:
        df_final = dim_df

    df_final.write.mode('overwrite').parquet(dim_path)
        

# dim_location
dim_location_df = spark.sql('SELECT Region, Country, City, ' \
'sha2(concat_ws("||", Region, Country, City), 256) AS Location_Id FROM (' \
'   SELECT DISTINCT Region, Country, City FROM sil_table' \
')')

dim_path = "hdfs://localhost:9005/data_lake/gold/dim_location"
dimension_table_insert(dim_path, dim_location_df, "Location_Id")

# dim_salesperson
dim_path = "hdfs://localhost:9005/data_lake/gold/dim_salesperson"
dim_salesperson_df = spark.sql('SELECT Sales_Person, sha2(concat_ws("||", Sales_Person), 256) AS Salesperson_Id ' \
        'FROM (SELECT DISTINCT Sales_Person FROM sil_table) unique_list'         
)
dimension_table_insert(dim_path, dim_salesperson_df, "Salesperson_Id")

# dim_customer
dim_path = "hdfs://localhost:9005/data_lake/gold/dim_customer"
dim_customer_df = spark.sql('WITH unique_customertype AS (' \
'SELECT Customer_Type FROM sil_table GROUP BY Customer_Type' \
')' \
'SELECT Customer_Type, sha2(concat_ws("||", Customer_Type), 256) AS Customertype_Id FROM unique_customertype')
dimension_table_insert(dim_path, dim_customer_df, "Customertype_Id")

# dim_channel
dim_path = "hdfs://localhost:9005/data_lake/gold/dim_channel"
dim_channel_df = spark.sql('WITH unique_channels AS(' \
'SELECT Sales_Channel FROM sil_table GROUP BY Sales_Channel' \
')' \
'SELECT Sales_Channel, sha2(concat_ws("||", Sales_Channel), 256) AS Channel_Id FROM unique_channels')
dimension_table_insert(dim_path, dim_channel_df, "Channel_Id")


# dim_promotion
dim_path = "hdfs://localhost:9005/data_lake/gold/dim_promotion"
dim_promotion_df = spark.sql('WITH unique_prom AS (SELECT Promotion_Type FROM sil_table GROUP BY Promotion_Type)' \
'SELECT Promotion_Type, sha2(concat_ws("||", Promotion_Type), 256) AS Promotion_Id FROM unique_prom')
dimension_table_insert(dim_path, dim_promotion_df, "Promotion_Id")

# dim_product
dim_path = "hdfs://localhost:9005/data_lake/gold/dim_product"
dim_product_df = spark.sql('WITH unique_pro AS (' \
'SELECT Product_Name, Product_Category, Brand FROM sil_table GROUP BY Product_Name, Product_Category, Brand' \
') ' \
'SELECT sha2(concat_ws("||", Product_Name, Product_Category, Brand), 256) AS Product_Id, Product_Name, Product_Category, Brand ' \
'FROM unique_pro'
)
dimension_table_insert(dim_path, dim_product_df, "Product_Id")

# dim_date
dim_path = "hdfs://localhost:9005/data_lake/gold/dim_date"
dim_date_df = spark.sql('WITH unique_date AS (' \
'SELECT DISTINCT Order_Date, Year, Month, Quarter, Month_Name FROM sil_table' \
')' \
'SELECT sha2(concat_ws("||", Order_Date, Year, Month, Quarter, Month_Name), 256) AS Date_Id, Order_Date, Year, Quarter, Month, Month_Name FROM unique_date')
dimension_table_insert(dim_path, dim_date_df, "Date_Id")



# Gold fact table
gold_fact = df.join(
    dim_location_df, 
    on = ['Region', 'Country', 'City'],
    how = 'left'
)
gold_fact = gold_fact.drop('Region', 'Country', 'City')

gold_fact = gold_fact.join(
    dim_salesperson_df,
    on = ['Sales_Person'],
    how = 'left'
)
gold_fact = gold_fact.drop('Sales_Person')

gold_fact = gold_fact.join(
    dim_customer_df,
    on = 'Customer_Type',
    how = 'left'
)
gold_fact = gold_fact.drop('Customer_Type')

gold_fact = gold_fact.join(
    dim_channel_df,
    on = 'Sales_Channel',
    how = 'left'
)
gold_fact = gold_fact.drop('Sales_Channel')

gold_fact = gold_fact.join(
    dim_promotion_df,
    on = 'Promotion_Type',
    how = 'left'
)
gold_fact = gold_fact.drop('Promotion_Type')

gold_fact = gold_fact.join(
    dim_product_df,
    on = ['Product_Name', 'Product_Category', 'Brand'],
    how = 'left'
)
gold_fact = gold_fact.drop('Product_Name', 'Product_Category', 'Brand')

gold_fact = gold_fact.join(
    dim_date_df,
    on = ['Order_Date', 'Year', 'Month', 'Quarter', 'Month_Name'],
    how = 'left'
)
gold_fact = gold_fact.drop('Order_Date', 'Year', 'Month', 'Quarter', 'Month_Name')

gold_fact.write.mode('append').parquet(gold_path + 'fact_table')

max_date = gold_fact.agg({"partition_date": "max"}).collect()[0][0]
with open(last_processed_file, 'w') as f:
    f.write(str(max_date))

print(f"Updated last processed date to: {max_date}")

spark.stop()
