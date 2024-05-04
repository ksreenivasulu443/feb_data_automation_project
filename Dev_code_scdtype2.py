from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SCD Type 2 Example") \
    .getOrCreate()

# Sample incoming data (you would read this from a source in reality)
incoming_data = spark.createDataFrame([
    (1, "John", "john@example.com", "2024-01-01"),
    (2, "Jane", "jane@example.com", "2024-01-01"),
    (3, "Bob", "bob@example.com", "2024-01-01")
], ["id", "name", "email", "effective_date"])

# Read existing dimension table
existing_data = spark.createDataFrame([
    (1, "John", "john@gmail.com", "2023-01-01", "9999-12-31"),
    (2, "ksr", "jane@example.com", "2023-01-01", "9999-12-31")
], ["id", "name", "email", "start_date", "end_date"])

# Join incoming data with existing data to identify changes
joined_data = incoming_data.join(existing_data,
                                 (incoming_data.id == existing_data.id) &
                                 (incoming_data.name != existing_data.name) &
                                 (incoming_data.email != existing_data.email),
                                 "left_outer") \
    .select(
        incoming_data.id,
        when(existing_data.id.isNull(), lit("INSERT")).otherwise(lit("UPDATE")).alias("action"),
        incoming_data.name,
        incoming_data.email,
        incoming_data.effective_date,
        existing_data.start_date,
        existing_data.end_date
    )

joined_data.show()

# Apply SCD Type 2 logic
updated_data = joined_data.select(
    col("id"),
    col("name"),
    col("email"),
    col("effective_date"),
    when(col("action") == "INSERT", lit("2024-01-01")).otherwise(col("start_date")).alias("start_date"),
    when(col("action") == "INSERT", lit("9999-12-31")).otherwise(col("end_date")).alias("end_date")
)

# Final result (you may write this to your target dimension table)
updated_data.show()
