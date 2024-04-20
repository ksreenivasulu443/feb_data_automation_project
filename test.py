from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]") \
    .appName("test") \
    .getOrCreate()

source = spark.read.csv(r"C:\Users\A4952\PycharmProjects\Data_Automation_project\source_files\Contact_info.csv", header=True, )

target= spark.read.csv(r"C:\Users\A4952\PycharmProjects\Data_Automation_project\source_files\Contact_info_t.csv", header=True)

print(target.schema.json())
#
#
# target.show(10)
#
#
# def schema_check(source, target):
#     source.createOrReplaceTempView("source")
#     target.createOrReplaceTempView("target")
#     spark.sql("describe source").createOrReplaceTempView("source_schema")
#     spark.sql("describe target").createOrReplaceTempView("target_schema")
#
#
#     failed = spark.sql('''select a.col_name source_col_name,b.col_name target_col_name, a.data_type as source_data_type, b.data_type as target_data_type,
#     case when a.data_type=b.data_type then "pass" else "fail" end status
#     from source_schema a full join target_schema b on a.col_name=b.col_name''').filter(" status = 'fail' ")
#
#     failed.show()
#
#
def name_check(target,column, pattern=None):
    pattern = r"^[a-zA-Z]+$"

    # Add a new column 'is_valid' indicating if the name contains only alphabetic characters
    df = target.withColumn("is_valid", regexp_extract(col(column), pattern, 0) != "")

    print("Show the DataFrame")
    df.filter('is_valid = False ').show()

#name_check(target,'Surname')
#
def column_range_check(target, column, min_value, max_value):
    print(column,min_value,max_value)
    filtered_dataframe = target.filter(f'{column} not between {min_value} and {max_value}')
    filtered_dataframe.show()

def column_value_reference_check(target, column, expected_values):
    expected_values = expected_values.split(",")
    print(column)
    print(expected_values)
    target.withColumn("is_present", col(column).isin(expected_values)).filter('is_present = False').show()

# column_range_check(target,'identifier', 5,10)
# column_value_reference_check(target, 'identifier', expected_values=[1,2,3,4,4,5])
#
#
# #schema_check(source, target)
# #data_compare(source, target, keycolumn='identifier')

spark.stop()