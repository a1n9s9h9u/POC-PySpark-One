import os
import sys

from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('My Join Functions App').getOrCreate()

emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
       (2, "Rose", 1, "2010", "20", "M", 4000),
       (3, "Williams", 1, "2010", "10", "M", 1000),
       (4, "Jones", 2, "2005", "10", "F", 2000),
       (5, "Brown", 2, "2010", "40", "", -1),
       (6, "Brown", 2, "2010", "50", "", -1)
       ]

empColumns = ["emp_id", "name", "superior_emp_id", "year_joined",
              "emp_dept_id", "gender", "salary"]

empDF = spark.createDataFrame(data=emp, schema=empColumns)

# empDF.printSchema()
# empDF.show()

dept = [("Finance", 10),
        ("Marketing", 20),
        ("Sales", 30),
        ("IT", 40)
        ]

deptColumns = ["dept_name", "dept_id"]

deptDF = spark.createDataFrame(data=dept, schema=deptColumns)

# deptDF.printSchema()
# deptDF.show()

# Inner join is the default join in PySpark and it’s mostly used.
# This joins two datasets on key columns, where keys don’t match the rows get dropped from both datasets
empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "inner").show()

# Outer a.k.a full, fullouter join returns all rows from both datasets,
# where join expression doesn’t match it returns null on respective record columns
empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "outer").show()

# Left a.k.a Leftouter join returns all rows from the left dataset regardless of
# match found on the right dataset when join expression doesn’t match,
# it assigns null for that record and drops records from right where match not found
empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "left").show()

# Right a.k.a Rightouter join is opposite of left join, here it returns all rows
# from the right dataset regardless of math found on the left dataset,
# when join expression doesn’t match, it assigns null for that record and drops
# records from left where match not found
empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "right").show()

# leftsemi join is similar to inner join difference being leftsemi join returns all columns
# from the left dataset and ignores all columns from the right dataset. In other words,
# this join returns columns from the only left dataset for the records match in the right
# dataset on join expression, records not matched on join expression are ignored from both left and right datasets

# The same result can be achieved using select on the result of the inner join however,
# using this join would be efficient
empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "leftsemi").show()

# leftanti join does the exact opposite of the leftsemi, leftanti join returns
# only columns from the left dataset for non-matched records
empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "leftanti").show()

# Joins are not complete without a self join, Though there is no self-join type available,
# we can use any of the above-explained join types to join DataFrame to itself.
# Below example use inner self join
empDF.alias("emp1").\
    join(empDF.alias("emp2"), col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner").\
    select(col("emp1.emp_id"), col("emp1.name"), col("emp2.emp_id"). \
           alias("superior_emp_id"), col("emp2.name").alias("superior_emp_name")).show()

# Since PySpark SQL support native SQL syntax, we can also write join operations
# after creating temporary tables on DataFrames and use these tables on spark.sql()
empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")

spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id").show()
spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id").show()
