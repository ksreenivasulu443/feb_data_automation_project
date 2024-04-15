from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

spark.read.csv(r"C:\Users\A4952\Downloads\Source_files\FIles\test.dat", header=True, sep='|').show()