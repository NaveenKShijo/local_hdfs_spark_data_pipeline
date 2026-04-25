from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession.builder.appName('new_app').config('spark.executor.memory', '4g').getOrCreate()

bronze_path =  "hdfs://localhost:9005/data_lake/bronze/unprocessed"
today = datetime.now()
silver_path = f"hdfs://localhost:9005/data_lake/silver/{today.year}/{today.month:02d}/{today.day:02d}"

df = spark.read.csv(bronze_path, header = True, inferSchema = True)

id_cols = ['Order_ID', 'SKU']
date_cols = ['Order_Date', 'Year', 'Quarter', 'Month', 'Month_Name']
categorical_cols = ['Region', 'Country', 'City', 'Sales_Person', 'Customer_Type', 'Sales_Channel', 'Promotion_Type', 'Product_Category','Brand', 'Product_Name']
float_cols = ['Unit_Price_USD', 'Discount_Pct', 'Gross_Sales_USD', 'Marketing_Spend_USD', 'COGS_USD', 'Logistics_Cost_USD', 'Net_Revenue_USD', 'Profit_USD', 'Profit_Margin_Pct']
int_cols = ['Units_Sold']

df = df.select([trim(col(c)).alias(c) for c in df.columns])
df = df.select([lower(col(c)).alias(c) for c in df.columns])

df = df.withColumn('Order_Date', to_timestamp(col('Order_Date'), 'yyyy-MM-dd'))


df = df.drop('Year', 'Quarter', 'Month', 'Month_Name')
df = df.withColumn('Year', year(col('Order_Date'))) \
        .withColumn('Month', month(col('Order_Date'))) \
        .withColumn('Quarter', quarter(col('Order_Date'))) \
        .withColumn('Month_Name', date_format(col('Order_Date'), 'MMMM')) \
        .withColumn('Day_Month', dayofmonth('Order_Date'))


df = df.select(*[col(c).cast('float').alias(c) if c in float_cols else col(c) for c in df.columns])
df = df.select(*[col(c).cast('int').alias(c) if c in int_cols else col(c) for c in df.columns])

unwanted_cols = ['SKU']
df = df.drop(*unwanted_cols)

df = df.dropDuplicates()


# df.write.mode("append").format("parquet").partitionBy("Year", "Month", "Day_Month") \
#         .save(silver_path)
# It splits the dataframe into multiple parquet files, one per unique combination of Year/Month/Day_Month.


df.write.mode('append').format("parquet").save(silver_path)
spark.stop()
