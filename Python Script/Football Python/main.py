from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("how to read csv file").getOrCreate()
df = spark.read.csv('Playerdata.csv',sep=',',inferSchema=True, header=True)
print(df)
