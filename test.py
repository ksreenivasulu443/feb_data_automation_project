from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]") \
    .appName("test") \
    .getOrCreate()

source = spark.read.csv(r"C:\Users\A4952\PycharmProjects\Data_Automation_project\source_files\Contact_info.csv", header=True, inferSchema=True)

target= spark.read.csv(r"C:\Users\A4952\PycharmProjects\Data_Automation_project\source_files\Contact_info_t.csv", header=True,inferSchema=True)

target.show(10)
def data_compare(source, target, keycolumn):
    print("*" * 50)
    print("Data compare validation has started ".center(50))
    print("*" * 50)

    keycolumn = keycolumn.split(",")

    keycolumn = [i.lower() for i in keycolumn]
    print(keycolumn)
    columnList = source.columns
    print(columnList)
    smt = source.exceptAll(target).withColumn("datafrom", lit("source"))
    tms = target.exceptAll(source).withColumn("datafrom", lit("target"))
    failed = smt.union(tms)
    failed2= failed.select(keycolumn).distinct().withColumn("hash_key", sha2(concat(*[col(c) for c in keycolumn]), 256))
    source = source.withColumn("hash_key", sha2(concat(*[col(c) for c in keycolumn]), 256)).join(failed2,["hash_key"], how='left_semi').drop('hash_key')
    target = target.withColumn("hash_key", sha2(concat(*[col(c) for c in keycolumn]), 256)).join(failed2, ["hash_key"],
                                                                                                 how='left_semi').drop('hash_key')
    source.show()
    target.show()
    print(source.count())
    print(target.count())
    if failed.count()>0:
        for column in columnList:
            print(column.lower())
            if column.lower() not in keycolumn:
                keycolumn.append(column)
                temp_source = source.select(keycolumn).withColumnRenamed(column, "source_" + column)
                temp_target = target.select(keycolumn).withColumnRenamed(column, "target_" + column)
                keycolumn.remove(column)
                temp_join = temp_source.join(temp_target, keycolumn, how='full_outer')
                temp_join.withColumn("comparison", when(col('source_' + column) == col("target_" + column),
                                                        "True").otherwise("False")).filter("comparison == False").show()
    print("*" * 50)
    print("Data compare validation has completed ".center(50))
    print("*" * 50)


def schema_check(source, target):
    source.createOrReplaceTempView("source")
    target.createOrReplaceTempView("target")
    spark.sql("describe source").createOrReplaceTempView("source_schema")
    spark.sql("describe target").createOrReplaceTempView("target_schema")


    failed = spark.sql('''select a.col_name source_col_name,b.col_name target_col_name, a.data_type as source_data_type, b.data_type as target_data_type, 
    case when a.data_type=b.data_type then "pass" else "fail" end status
    from source_schema a full join target_schema b on a.col_name=b.col_name''').filter(" status = 'fail' ")

    failed.show()


def name_check(target,column):
    pattern = r"^[a-zA-Z]+$"

    # Add a new column 'is_valid' indicating if the name contains only alphabetic characters
    df = target.withColumn("is_valid", regexp_extract(col(column), pattern, 0) != "")

    print("Show the DataFrame")
    df.filter('is_valid = False ').show()

name_check(target,'Surname')


#schema_check(source, target)
#data_compare(source, target, keycolumn='identifier')