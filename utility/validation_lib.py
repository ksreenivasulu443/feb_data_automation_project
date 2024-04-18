
def count_check(source, target,Out,row, validation):
    source_count = source.count()
    target_count = target.count()
    failed = source_count-target_count
    print("source count", source_count)
    print("target coungt", target_count)
    if source_count == target_count:
        print("count is matching")
        write_output(validation_Type = validation, source = row['source'], target=row['target'],
                     Number_of_source_Records = source_count, Number_of_target_Records = target_count,
                 Number_of_failed_Records = failed, column= row['key_col_list'], Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'],Out=Out)
    else:
        print("count is not matching and diff is" , abs(source_count-target_count))
        write_output(validation_Type=validation, source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=row['key_col_list'], Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)

def duplicate_check(target,key_column,Out,row,validation):
    key_column = key_column.split(",")
    duplicate = target.groupBy(key_column).count().where('count>1')
    duplicate.show()
    target_count = target.count()
    failed = duplicate.count()
    if failed > 0:
        print("Duplicates present")
        write_output(validation_Type=validation, source=row['source'], target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=row['key_col_list'], Status='FAIL',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)
    else:
        print("duplicates not present")
        write_output(validation_Type=validation, source=row['source'], target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=row['key_col_list'], Status='PASS',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)

def uniqueness_check(target : str,
                     unique_col_list: list,
                     Out : dict,
                     row : dict,
                     validation: str):
    unique_col_list = unique_col_list.split(",")
    target_count = target.count()
    for col in unique_col_list:
        #print(col*50)
        unique_values = target.groupBy(col).count().where('count>1')

        failed = unique_values.count()

        if failed > 0 :
            print("duplicates present on ", col)
            write_output(validation_Type=validation, source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=failed, column=col, Status='FAIL',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)
        else:
            print("Duplicates not present",col)
            write_output(validation_Type=validation, source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=0, column=col, Status='PASS',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)


def records_present_only_in_target(source, target, keyList: str, Out, row, validation):
    columns = keyList
    keyList = keyList.split(",")
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    failed = count_compare.filter("SourceCount is null or SourceCount != TargetCount ").count()
    print("Key column record present in target but not in Source :" + str(failed))
    source_count = source.count()
    target_count = target.count()
    if failed > 0:
        write_output(validation_Type=validation, source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=columns, Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type=validation, source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=columns, Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)



def records_present_only_in_source(source, target, keyList: str, Out, row, validation):
    columns = keyList
    columns = row['key_col_list']
    keyList = keyList.split(",")
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    failed = count_compare.filter("TargetCount is null or SourceCount != TargetCount ").count()
    print("Key column record present in target but not in Source :" + str(failed))
    source_count = source.count()
    target_count = target.count()
    if failed > 0:
        write_output(validation_Type=validation, source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=columns, Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type=validation, source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=columns, Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)


def null_value_check(target, Null_columns, Out, row):

    target_count = target.count()
    Null_columns = Null_columns.split(",")
    for column in Null_columns:
        Null_df = target.select(count(when(col(column).contains('None') |
                                              upper(col(column)).contains('NULL') |
                                              upper(col(column)).contains('NA') | na, Na, NA, nA
                                              (col(column) == '') |
                                              col(column).isNull() |
                                              isnan(column), column
                                              )).alias("Null_value_count"))
        # dataframe.createOrReplaceTempView("dataframe")
        # Null_df = spark.sql(f"select count(*) source_cnt from dataframe where {column} is null")
        failed = Null_df.collect()[0][0]

        if failed > 0:


def write_output(validation_Type, source, target, Number_of_source_Records, Number_of_target_Records,
                 Number_of_failed_Records, column, Status,source_type,target_type,Out):
    Out["Source_name"].append(source)
    Out["target_name"].append(target)
    Out["column"].append(column)
    Out["validation_Type"].append(validation_Type)
    Out["Number_of_source_Records"].append(Number_of_source_Records)
    Out["Number_of_target_Records"].append(Number_of_target_Records)
    Out["Status"].append(Status)
    Out["Number_of_failed_Records"].append(Number_of_failed_Records)
    Out["source_type"].append(source_type)
    Out["target_type"].append(target_type)









def schema(source, target,spark):
    source.createOrReplaceTempView('source')
    target.createOrReplaceTempView('target')

    spark.sql("describe source").show()
    spark.sql("describe target").show()
