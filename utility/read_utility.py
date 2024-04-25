import json

from pyspark.sql import SparkSession
from utility.general_utility import flatten

def read_file(type: str,
              path: str,
              spark: SparkSession,
              row,
              schema: str = 'NOT APPL',
              multiline: bool = True,
              ):
    try:
        type = type.lower()
        if type == 'csv':
            if schema != 'NOT APPL':
                df = spark.read.schema(schema).csv(path)
            else:
                df = (spark.read.option("inferSchema", True).
                      option("header", True).option("delimiter", ",").csv(path))
        elif type == 'json':
            if multiline == True:
                df = spark.read.option("multiline", True).json(path)
                df = flatten(df)
            else:
                df = spark.read.option("multiple", False).json(path)
                df = flatten(df)
        elif type == 'parquet':
            df = spark.read.parquet(path)
        elif type == 'avro':
            df = spark.read.format('avro').load("path")
        elif type == 'text':
            df = spark.read.format("text").load(path)
        elif type == 'orc':
            pass
        else:
            raise ValueError("Unsupported file format", type)
        words = row['exclude_columns'].split(',')
        exclude_cols = ",".join(words)
        #return df.drop('batch_date','create_date','update_date','create_user','update_user')
        return df.drop(exclude_cols)
    except FileNotFoundError as e:
        df = None

    except Exception as e:
        df = None


def read_db(spark: SparkSession,
            table: str,
            database: str,
            query: str,
            row):
    try:
        with open(r"C:\Users\A4952\PycharmProjects\feb_data_automation_project\config\Config.json") as f:
            config_data = json.load(f)[database]
            print(config_data)
        if query != 'NOT APPL':
            with open(query, "r") as file:
                sql_query = file.read()
            print(sql_query)
            print(config_data)
            df = spark.read.format("jdbc"). \
                option("url", config_data['url']). \
                option("user", config_data['user']). \
                option("password", config_data['password']). \
                option("query", sql_query). \
                option("driver", config_data['driver']).load()
        else:
            df = spark.read.format("jdbc"). \
                option("url", config_data['url']). \
                option("user", config_data['user']). \
                option("password", config_data['password']). \
                option("dbtable", table). \
                option("driver", config_data['driver']).load()

        return df.drop('batch_date','create_date','update_date','create_user','update_user')

    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")
        return None
    except KeyError as e:
        print(f"Key error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def read_snowflake(spark: SparkSession,
            table: str,
            database: str,
            query: str, row):
    try:
        with open(r"C:\Users\A4952\PycharmProjects\feb_data_automation_project\config\Config.json") as f:
            config_data = json.load(f)[database]
            print(config_data)
        if query != 'NOT APPL':
            with open(query, "r") as file:
                sql_query = file.read()
            print(sql_query)
            print(config_data)
            df = spark.read \
                .format("jdbc") \
                .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
                .option("url", config_data['jdbc_url']) \
                .option("query", sql_query) \
                .load()
        else:
            df = spark.read \
                .format("jdbc") \
                .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
                .option("url", config_data['jdbc_url']) \
                .option("dbtable", table) \
                .load()

        return  df.drop('batch_date','create_date','update_date','create_user','update_user')
    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")
        return None
    except KeyError as e:
        print(f"Key error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None



