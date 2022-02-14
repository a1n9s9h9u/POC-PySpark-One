import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

from pyspark.sql.window import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('My Window Functions App').getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]

schema = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data=simpleData, schema = schema)

# df.printSchema()
# df.show()

windowSpec = Window.partitionBy("department").orderBy("salary")

# row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition
df.withColumn("row_number",row_number().over(windowSpec)).show()

# rank() window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.
df.withColumn("rank",rank().over(windowSpec)).show()

# dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps. This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.
df.withColumn("dense_rank",dense_rank().over(windowSpec)).show()

# ntile() window function returns the relative rank of result rows within a window partition. In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)
df.withColumn("ntile",ntile(2).over(windowSpec)).show()

# cume_dist() window function is used to get the cumulative distribution of values within a window partition
df.withColumn("cume_dist",cume_dist().over(windowSpec)).show()

# When working with Aggregate functions, we don’t need to use order by clause

windowSpecAgg = Window.partitionBy("department")

df.withColumn("row",row_number().over(windowSpec)).show()

df.withColumn("avg", avg(col("salary")).over(windowSpecAgg)).show()
