import os
import sys

from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

# import pandas as pd

spark = SparkSession.builder.appName('My First App').getOrCreate()

# print(spark)

# data_xlsx = pd.read_excel('bank.xlsx', 'Sheet1')
# data_xlsx.to_csv('bankcsv.csv', encoding='utf-8', index=False)

df = spark.read.csv("bankcsv.csv", inferSchema=True, header=True)
# df.show(5)

# Fixing Column names, removing whitespaces
new_column_name_list = list(map(lambda x: x.replace(" ", "_"), df.columns))
df = df.toDF(*new_column_name_list)
df = df.withColumnRenamed("CHQ.NO.", "CHQ_NO")

# df.show(2)
df.printSchema()

df = df.drop(".")

# df.show(3)
# df.printSchema()

# Adding new TransactionAmount Column

df = df.withColumn('TransactionAmount', when(df['WITHDRAWAL_AMT'].isNull(), df['DEPOSIT_AMT']).when(df['DEPOSIT_AMT'].isNull(), df['WITHDRAWAL_AMT']).otherwise(0))

# df.show(5)
# df.printSchema()

# Adding new TransactionType Column
df = df.withColumn('TransactionType', when(df.WITHDRAWAL_AMT.isNull(), "CR").when(df.DEPOSIT_AMT.isNull(), "DR"))
# df.show(5)

dfnew = df.select(df.Account_No, df.DATE, df.TRANSACTION_DETAILS, df.CHQ_NO, df.VALUE_DATE, df.WITHDRAWAL_AMT, df.DEPOSIT_AMT, df.TransactionType, df.TransactionAmount, df.BALANCE_AMT)

# dfnew.show(10)

# dfnew.repartition(1).write.mode("overwrite").option("header", "true").csv("updatedbank")

# opening saved file in a new DF
dfupdated = spark.read.csv('updatedbank', inferSchema = 'true', header = 'true')

# Selecting all transactions done by Cheque
chequeTransactions = dfupdated.filter(dfupdated.CHQ_NO.isNotNull())
# chequeTransactions.show(10)
# chequeTransactions.write.mode("overwrite").option("header", "true").parquet("CheckTransactions")

# Selecting all transactions done by CR
crTransactions = dfupdated.filter(dfupdated.TransactionType == "CR")
# crTransactions.show(5)
# crTransactions.write.mode("overwrite").option("header", "true").parquet("CrTransactions")

# Selecting all transactions done by DR
drTransactions = dfupdated.filter(dfupdated.TransactionType == "DR")
# drTransactions.show(5)
# drTransactions.write.mode("overwrite").option("header", "true").parquet("DrTransactions")

dfupdatednew = spark.read.csv('updatedbank', inferSchema = 'true', header = 'true')
# dfupdatednew.printSchema()
# dfupdatednew.show()

# Transactions for a Day
dfupdatednew.filter(dfupdatednew["DATE"] == lit("2017-08-16")).show()

# Transactions for a range of date
date_from, date_to = "2017-08-16",  "2018-08-16"
dfupdatednew.filter(dfupdatednew["DATE"] >= date_from).filter(dfupdatednew["DATE"] <= date_to).show(100)

# Identify duplicate transactions
dfDuplicateTransactions = dfupdatednew.groupBy("Account_No", "DATE", "TransactionType", "TransactionAmount").count().filter("count > 1")
dfDuplicateTransactions.repartition(1).write.mode("overwrite").option("header", "true").csv("dfDuplicateTransactions")

# Get total deposit and withdrawal done for each account in a range of date
date_from, date_to = "2017-08-16",  "2018-08-16"
dfUpdatedRange = dfupdatednew.filter(dfupdatednew["DATE"] >= date_from).filter(dfupdatednew["DATE"] <= date_to)

dfDropDuplicateTransaction = dfupdatednew.dropDuplicates(["Account_No", "DATE", "TransactionType", "TransactionAmount"])
dfDropDuplicateTransaction.show()

dfSumOfWithdrawalDeposit = dfDropDuplicateTransaction.groupBy("Account_No").sum("WITHDRAWAL_AMT", "DEPOSIT_AMT")
dfSumOfWithdrawalDeposit.show()
dfSumOfWithdrawalDeposit.repartition(1).write.mode("overwrite").option("header", "true").csv("SumOfWithdrawalDeposit")

# Change the precision of the decimal vlue
df201 = dfupdatednew.groupBy("Account_No").sum("WITHDRAWAL_AMT").withColumnRenamed("sum(WITHDRAWAL_AMT)", "Total_Withdrawal")
df202 = df201.withColumn("Total_Withdrawal", df201.Total_Withdrawal.cast(DecimalType(18, 2)))
df202.show()
df202.printSchema()

