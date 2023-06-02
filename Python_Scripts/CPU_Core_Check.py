# Pyspark sql, Pyspark Pandas, Pandas File run in 1 cpu core 

import datetime as dt
import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession
# from pyspark.sql.functions import pandas_udf, PandasUDFType
# from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Create a SparkSession
# spark = SparkSession.builder.master("local[1]").appName("TimingComparison").config("spark.executor.instances","1").config("spark.cores.max","1").config("spark.executor.cores","1").config("spark.driver.cores","1").config("spark.task.cpus","1").getOrCreate()
spark = SparkSession.builder.master("local").appName("TimingComparison").getOrCreate()


# spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false")
# spark.conf.set("spark.executor.instances","1")
# spark.conf.set("spark.cores.max","1")
# spark.conf.set("spark.executor.cores","1")
# spark.conf.set("spark.driver.cores","1")
# spark.conf.set("spark.task.cpus","1")

# Reading dummy file
# start_time = dt.datetime.now()
# spark_df = spark.read.csv("FactSet_Position_202212124.csv", header=True, inferSchema=True)


# Reading using Spark.sql
start_time = dt.datetime.now()
spark_df = spark.read.csv("FactSet_Position_202212124_390K.csv", header=True, inferSchema=True)
print(f"Spark SQL time: {(dt.datetime.now() - start_time).total_seconds()} seconds")


# Reading using pyspark.pandas
# ps.set_option('compute.default_index_type', 'sequence')
# start_time = dt.datetime.now()
# spark_pandas_df = ps.read_csv("FactSet_Position_202212124_390K.csv")
# print(f"Spark Pandas time: {(dt.datetime.now() - start_time).total_seconds()} seconds")


# Reading using simple pandas
# start_time = dt.datetime.now()
# pandas_df = pd.read_csv("FactSet_Position_202212124_390K.csv")
# print(f"Pandas time: {(dt.datetime.now() - start_time).total_seconds()} seconds")

# To run file in a single cpu core
# taskset -c 1 spark-submit pysqlCheck.py