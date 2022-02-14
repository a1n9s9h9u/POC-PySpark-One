import os
import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('My User Defined Functions App').getOrCreate()

# UDF’s are used to extend the functions of the framework and re-use these functions on multiple DataFrame’s. For
# example, you wanted to convert every first letter of a word in a name string to a capital case; PySpark build-in
# features don’t have this function hence you can create it a UDF and reuse this as needed on many Data Frames.

columns = ["Seqno","Name"]

data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

# df.show()

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

# Converting function to UDF

convertUDF = udf(lambda z: convertCase(z),StringType())

df.select(col("Seqno"), convertUDF(col("Name")).alias("Name") ).show()

def upperCase(str):
    return str.upper()

upperCaseUDF = udf(lambda z:upperCase(z),StringType())

df.withColumn("Uppercase Name", upperCaseUDF(col("Name"))).show()
