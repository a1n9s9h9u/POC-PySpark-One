import os
import sys

from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('My Aggregate Functions App').getOrCreate()

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

# approx_count_distinct() function returns the count of distinct items in a group
df.select(approx_count_distinct("salary")).show()

# avg() function returns the average of values in the input column
df.select(avg("salary")).show()

# collect_list() function returns all values from an input column with duplicates
df.select(collect_list("salary")).show()

# collect_set() function returns all values from an input column with duplicate values eliminated
df.select(collect_set("salary")).show()

# countDistinct() function returns the number of distinct elements in a columns
df.select(countDistinct("department", "salary")).show()

# count() function returns number of elements in a column
df.select(count("salary")).show()

# first() function returns the first element in a column
df.select(first("salary")).show()

# last() function returns the last element in a column
df.select(last("salary")).show()

# kurtosis() function returns the kurtosis of the values in a group
df.select(kurtosis("salary")).show()

# max() function returns the maximum value in a column
df.select(max("salary")).show()

# min() function
df.select(min("salary")).show()

# mean() function returns the average of the values in a column. Alias for Avg
df.select(mean("salary")).show()

# sum() function Returns the sum of all values in a column
df.select(sum("salary")).show()

# sumDistinct() function returns the sum of all distinct values in a column
df.select(sumDistinct("salary")).show()
