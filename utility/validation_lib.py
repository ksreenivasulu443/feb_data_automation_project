
def count_check(source, target):
    source_count = source.count()
    target_count = target.count()
    print("source count", source_count)
    print("target coungt", target_count)
    if source_count == target_count:
        print("count is matching")
    else:
        print("count is not matching and diff is" , abs(source_count-target_count))

def duplicate_check(target,key_column):
    key_column = key_column.split(",")
    #target.createOrReplaceTempView("target")
    #spark.sql(f" select {key_column}, count(1) from target group by all having count(1)>1")
    duplicate = target.groupBy(key_column).count().where('count>1')
    duplicate.show()
    target_count = target.count()
    failed = duplicate.count()
    if failed > 0:
        print("Duplicates present")
    else:
        print("duplicates not present")