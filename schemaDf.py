import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('My Schema App').getOrCreate()

schema = StructType([
    StructField("Account No", StringType()),
    StructField("DATE", StringType()),
    StructField("TRANSACTION DETAILS", StringType()),
    StructField("CHQ.NO.", StringType()),
    StructField("VALUE DATE", StringType()),
    StructField("WITHDRAWAL AMT", DoubleType()),
    StructField("DEPOSIT AMT", DoubleType()),
    StructField("BALANCE AMT", DoubleType()),
    StructField(".", IntegerType())
])

df = spark.read.csv("bankcsv.csv", inferSchema=True, header=True)
df.printSchema()
df.show()

dfSchema = spark.read.csv("bankcsv.csv", header=True, schema=schema)
dfSchema.printSchema()
dfSchema.show()
