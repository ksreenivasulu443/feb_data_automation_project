import pandas as pd
import openpyxl
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
from utility.read_utility import read_file, read_db

project_path = os.getcwd()

jar_path = project_path + "/jars/postgresql-42.2.5.jar"
spark = SparkSession.builder.master("local[2]") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()

project_path = os.getcwd()
template_path = project_path + '\config\Master_Test_Template.xlsx'
test_cases = pd.read_excel(template_path)

run_test_case = test_cases.loc[(test_cases.execution_ind == 'Y')]

run_test_case = spark.createDataFrame(run_test_case)

validation = (run_test_case.groupBy('source', 'source_type',
                                    'source_db_name', 'schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_db_name',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list',
                                    'unique_col_list').
              agg(collect_set('validation_Type').alias('validation_Type')))

validation.show(truncate=False)

validations = validation.collect()

for row in validations:
    print(row['source'])
    print(row['target'])
    print(row['target_type'])
    print(row['source_db_name'])
    print(row['source_transformation_query_path'])

    if row['source_type'] == 'table':
        source = read_db(spark=spark,
                         table=row['source'],
                         database=row['source_db_name'],
                         query=row['source_transformation_query_path'])

    else:
        print("schema",row['schema_path'])
        source = read_file(type=row['source_type'],
                           path=row['source'],
                           spark=spark,
                           schema=row['schema_path'])

    if row['target_type'] == 'table':
        target = read_db(spark=spark,
                         table=row['target'],
                         database=row['target_db_name'],
                         query=row['target_transformation_query_path'])

    else:
        target = read_file(type=row['target_type'],
                           path=row['target'],
                           spark=spark,
                           schema=row['schema_path'])

    source.show()
    target.show()
