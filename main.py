import pandas as pd
import openpyxl
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
from utility.read_utility import read_file, read_db
from utility.validation_lib import count_check, duplicate_check, uniqueness_check, records_present_only_in_source, \
    null_value_check, data_compare,name_check,column_range_check,column_value_reference_check

project_path = os.getcwd()

jar_path = project_path + "/jars/postgresql-42.2.5.jar"
spark = SparkSession.builder.master("local[1]") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()

project_path = os.getcwd()
template_path = project_path + '\config\Master_Test_Template.xlsx'
test_cases = pd.read_excel(template_path)

Out = {
    "validation_Type": [],
    "Source_name": [],
    "target_name": [],
    "Number_of_source_Records": [],
    "Number_of_target_Records": [],
    "Number_of_failed_Records": [],
    "column": [],
    "Status": [],
    "source_type": [],
    "target_type": []
}

run_test_case = test_cases.loc[(test_cases.execution_ind == 'Y')]

run_test_case = spark.createDataFrame(run_test_case)

validation = (run_test_case.groupBy('source', 'source_type',
                                    'source_db_name', 'schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_db_name',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list',
                                    'unique_col_list','dq_column','expected_values','min_val','max_val').
              agg(collect_set('validation_Type').alias('validation_Type')))

validation.show(truncate=False)

validations = validation.collect()

for row in validations:

    if row['source_type'] == 'table':
        source = read_db(spark=spark,
                         table=row['source'],
                         database=row['source_db_name'],
                         query=row['source_transformation_query_path'])

    else:
        print("schema", row['schema_path'])
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

    for validation in row['validation_Type']:
        if validation == 'count_check':
            count_check(source, target, Out, row, validation)
        elif validation == 'duplicate_check':
            duplicate_check(target, row['key_col_list'], Out, row, validation)
        elif validation == 'uniqueness_check':
            uniqueness_check(target, row['unique_col_list'], Out, row, validation)
        elif validation == 'records_present_only_in_source':
            records_present_only_in_source(source, target, row['key_col_list'], Out, row, validation)
        elif validation == 'records_present_only_target':
            records_present_only_in_source(source, target, row['key_col_list'], Out, row, validation)
        elif validation == 'null_value_check':
            null_value_check(target, row['null_col_list'], Out, row, validation)
        elif validation == 'data_compare':
            data_compare(source, target, row['key_col_list'], Out, row, validation)
        elif validation == 'name_check':
            name_check(target,row['dq_column'],Out, row, validation,)
        elif validation == 'column_range_check':
            column_range_check(target,row['dq_column'],row['min_val'],row['max_val'],validation, row, Out)
        elif validation == 'column_value_reference_check':
            column_value_reference_check(target,row['dq_column'],row['expected_values'],Out, row, validation)

print(Out)

summary = pd.DataFrame(Out)

print(summary)

summary.to_csv(r"C:\Users\A4952\PycharmProjects\feb_data_automation_project\execution_summary\summary.csv")

spark.stop()
