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
oracle_jar = project_path + "/jars/ojdbc11.jar"

jar_path = postgre_jar+','+snow_jar + ','+oracle_jar
spark = SparkSession.builder.master("local[2]") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()
system_user = getpass.getuser()

batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
# Read customer data from a source (e.g., CSV file)
customer_df = spark.read.csv(r"C:\Users\A4952\PycharmProjects\feb_data_automation_project\scd_type2_source_files", header=True, inferSchema=True)

customer_df= customer_df \
    .withColumn('create_date', current_timestamp())\
    .withColumn('update_date', current_timestamp())\
    .withColumn('create_user',lit(system_user))\
    .withColumn('update_user',lit(system_user))

customer_df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "customer_raw") \
    .option("user", "postgres") \
    .option("password", "Dharmavaram1@") \
    .option("driver", 'org.postgresql.Driver') \
    .save()

customer_df.show()

spark.stop()