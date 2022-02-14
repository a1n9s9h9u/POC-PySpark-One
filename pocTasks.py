import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

# import pandas as pd

spark = SparkSession \
    .builder \
    .appName("PySpark Project") \
    .getOrCreate()

# data_xlsx = pd.read_excel('bank.xlsx', 'Sheet1')
# data_xlsx.to_csv('bank_csv.csv', encoding='utf-8', index=False)

df = spark.read.csv("bank_csv.csv", inferSchema=True, header=True)

# Fixing Column names, removing whitespaces
new_column_name_list = list(map(lambda x: x.replace(" ", "_"), df.columns))
df = df.toDF(*new_column_name_list)
df = df.withColumnRenamed("CHQ.NO.", "CHQ_NO")

# df.show(2)
# df.printSchema()

df = df.drop(".")

# df.show(3)
# df.printSchema()

# Adding new TransactionAmount Column

df = df.withColumn('TransactionAmount',
                   when(df['WITHDRAWAL_AMT'].isNull(), df['DEPOSIT_AMT']).when(df['DEPOSIT_AMT'].isNull(),
                                                                               df['WITHDRAWAL_AMT']).otherwise(0))

# df.show(5)
# df.printSchema()

# Adding new TransactionType Column
df = df.withColumn('TransactionType', when(df.WITHDRAWAL_AMT.isNull(), "CR").when(df.DEPOSIT_AMT.isNull(), "DR"))
# df.show(5)

dfnew = df.select(df.Account_No, df.DATE, df.TRANSACTION_DETAILS, df.CHQ_NO, df.VALUE_DATE, df.WITHDRAWAL_AMT,
                  df.DEPOSIT_AMT, df.TransactionType, df.TransactionAmount, df.BALANCE_AMT)

# dfnew.show(10)

dfnew.repartition(1).write.mode("overwrite").option("header", "true").csv("updatedbank")

dfnew.coalesce(1).write.mode("overwrite").option("header", "true").csv("bank")

# opening saved file in a new DF
dfupdated = spark.read.csv('updatedbank', inferSchema='true', header='true')

# Selecting all transactions done by Cheque
chequeTransactions = dfupdated.filter(dfupdated.CHQ_NO.isNotNull()).show()

# chequeTransactions.show(10)

# Exporting to Parquet file
chequeTransactions.write.mode("overwrite").option("header", "true").parquet("CheckTransactions")
# chequeTransactions.repartition(1).write.mode("overwrite").option("header", "true").parquet("CheckTransactions")

# Selecting all DR transactions using SQL Query
dfupdated.createOrReplaceTempView("temp")
drTransactions = spark.sql("SELECT * from temp where TransactionType == 'DR'")

# drTransactions.show(10)

# dropping temp table
spark.catalog.dropTempView("temp")

drTransactions.write.mode("overwrite").option("header", "true").parquet("DRTransactions")
# drTransactions.coalesce(1).write.mode("overwrite").option("header", "true").parquet("DRTransactions")

# Selecting all CR transactions
crTransactions = dfupdated.select("*").where(dfupdated.TransactionType == 'CR')

# crTransactions.show(10)

crTransactions.write.mode("overwrite").option("header", "true").parquet("CRTransactions")
# crTransactions.coalesce(1).write.mode("overwrite").option("header", "true").parquet("CRTransactions")
