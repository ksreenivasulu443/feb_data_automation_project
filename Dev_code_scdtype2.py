import datetime
import json
import os
import sys
import pandas as pd
from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode, current_timestamp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import getpass
os.environ.setdefault("project_path", os.getcwd())
project_path = os.environ.get("project_path")


# jar_path = pkg_resources.resource_filename('jars', 'postgresql-42.2.5.jar')
postgre_jar = project_path + "/jars/postgresql-42.2.5.jar"
snow_jar = project_path + "/jars/snowflake-jdbc-3.14.3.jar"


jar_path = postgre_jar+','+snow_jar
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

#customer_df = spark.read.csv(r"C:\Users\A4952\PycharmProjects\feb_data_automation_project\scd_type2_source_files", header=True, inferSchema=True)

customer_df.show()


window_spec = Window.partitionBy("customer_id").orderBy("batch_date")

# Add start_date and end_date columns to customer_df
customer_df = customer_df.withColumn("start_date", F.first("batch_date").over(window_spec)) \
    .withColumn("end_date", F.lead("batch_date", 1).over(window_spec))

customer_df.show()
# Replace null end_date values with a high date (e.g., '9999-12-31')
customer_df = customer_df.withColumn("end_date", F.coalesce("end_date", F.lit("9999-12-31")))
customer_df.show()

# Add a column to indicate whether there is a change in firstname, lastname, or email
customer_df = customer_df.withColumn("change_detected",
                                     F.when((F.col("first_name") != F.lag("first_name", 1).over(window_spec)) |
                                            (F.col("last_name") != F.lag("last_name", 1).over(window_spec)) |
                                            (F.col("email") != F.lag("email", 1).over(window_spec)),
                                            F.lit(1)).otherwise(F.lit(0)))


customer_df.show()

# Filter only the records where change is detected or it's the first record for a customer
customer_df_scd2 = customer_df.filter((F.col("change_detected") == 1) | (F.col("start_date") == F.col("batch_date")))
customer_df_scd2=customer_df_scd2.drop('change_detected')
customer_df_scd2.show()
customer_df_scd2.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "customer") \
    .option("user", "postgres") \
    .option("password", "Dharmavaram1@") \
    .option("driver", 'org.postgresql.Driver') \
    .save()


spark.stop()