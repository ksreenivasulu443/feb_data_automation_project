import datetime
import json
import os
import sys
import pandas as pd
from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode, current_timestamp
from pyspark.sql import SparkSession
import getpass
os.environ.setdefault("project_path", os.getcwd())
project_path = os.environ.get("project_path")




# jar_path = pkg_resources.resource_filename('jars', 'postgresql-42.2.5.jar')
postgre_jar = project_path + "/jars/postgresql-42.2.5.jar"
snow_jar = project_path + "/jars/snowflake-jdbc-3.14.3.jar"

jar_path = postgre_jar+','+snow_jar
spark = SparkSession.builder.master("local[5]") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()


# Fetch system user's login name
system_user = getpass.getuser()

batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


# Load from File to DB raw table

file = spark.read.csv(r"C:\Users\A4952\PycharmProjects\feb_data_automation_project\source_files\sales_data.csv", header=True, inferSchema=True)

file = file.withColumn('batch_date', lit(batch_id))\
    .withColumn('create_date', current_timestamp())\
    .withColumn('create_user',lit(system_user))





url = 'jdbc:snowflake://wbaiyzo-as58233.snowflakecomputing.com/?user=KATSREEN100&password=Dharmavaram1@&warehouse=COMPUTE_WH&db=ETL_AUTO&schema=CONTACT_INFO'

# file.write.mode("overwrite") \
#     .format("jdbc") \
#     .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
#     .option("url", url) \
#     .option("dbtable", "ETL_AUTO.ECOM.SALES") \
#     .save()

aggregated_data = file.groupBy('product', 'location').agg(
    sum('quantity').alias('total_quantity'),
    sum('amount').alias('total_amount'),
    mean('price').alias('average_price')
)

aggregated_data.show()

aggregated_data.write.mode("overwrite") \
    .format("jdbc") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .option("url", url) \
    .option("dbtable", "ETL_AUTO.ECOM.SALES_AGGREGATE") \
    .save()


spark.stop()

