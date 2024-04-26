from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create or get SparkSession
spark = SparkSession.builder \
    .appName("SCD Type 2") \
    .getOrCreate()

# Load data
customer_df = spark.read.csv(r"C:\Users\A4952\PycharmProjects\feb_data_automation_project\scd_type2_source_files", header=True, inferSchema=True)

# Define window specification
window_spec = Window.partitionBy("customer_id").orderBy("batch_date")

# Add start_date and end_date columns to customer_df
customer_df = customer_df.withColumn("start_date", F.lag("batch_date", 1).over(window_spec) + F.expr('INTERVAL 1 DAY')) \
    .withColumn("end_date", F.lead("batch_date", 1).over(window_spec) - F.expr('INTERVAL 1 DAY'))

# Replace null end_date values with a high date (e.g., '9999-12-31')
customer_df = customer_df.withColumn("end_date", F.coalesce("end_date", F.lit("9999-12-31")))

# Add a column to indicate whether there is a change in firstname, lastname, or email
customer_df = customer_df.withColumn("change_detected",
                                     F.when((F.col("first_name") != F.lag("first_name", 1).over(window_spec)) |
                                            (F.col("last_name") != F.lag("last_name", 1).over(window_spec)) |
                                            (F.col("email") != F.lag("email", 1).over(window_spec)),
                                            F.lit(1)).otherwise(F.lit(0)))

# Create a new column for version number
customer_df = customer_df.withColumn("version", F.row_number().over(window_spec))
customer_df.show()
# Filter only the records where change is detected or it's the first record for a customer
customer_df_scd2 = customer_df.filter((F.col("change_detected") == 1) | (F.col("start_date") == F.col("batch_date")))
customer_df_scd2 = customer_df_scd2.drop('change_detected')

customer_df_scd2.show()
