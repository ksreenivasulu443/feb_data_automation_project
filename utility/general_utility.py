from pyspark.sql.types import *
from pyspark.sql.functions import *

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[2]") \
#     .appName("test") \
#     .getOrCreate()

import os
import json
def flatten(df):
    # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [col(col_name + '.' + k).alias( k) for k in
                        [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))



        # recompute remaining Complex Fields in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df
given_path = os.path.abspath(os.path.dirname(__file__))
def read_config(database):
    parent_path = os.path.dirname(given_path) + '\Config\Config.json'
    # Read the JSON configuration file
    with open(parent_path) as f:
        config = json.load(f)[database]
    return config

def read_schema(schema_file_path):
    path = os.path.dirname(given_path) + '\\schema\\' +schema_file_path
    # Read the JSON configuration file
    print(path)
    with open(path, 'r') as schema_file:
        schema = StructType.fromJson(json.load(schema_file))
        print(schema)
    return schema


# schema= read_schema('contact_info_schema.json')
# df = (spark.read.schema(schema).option("header", True).option("delimiter", ",").
#       csv(r"C:\Users\A4952\PycharmProjects\feb_data_automation_project\source_files\contact_info_20240424.csv"))
# df.show()

def fetch_transformation_query_path(file_path):
    path = os.path.dirname(given_path) + '/Transformations_queries/' + file_path
    with open(path, "r") as file:
        sql_query = file.read()
    return sql_query

def fetch_transformation_query_path(file_path):
    path = os.path.dirname(given_path) + '/Transformations_queries/' + file_path
    with open(path, "r") as file:
        sql_query = file.read()
    return sql_query

def fetch_file_path(file_path):
    path = os.path.dirname(given_path) + '/source_files/'+file_path
    return path