import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('My POC One App').getOrCreate()


# csv to dataframe
def readCsv(csvfile):
    dataframe = spark.read.csv(csvfile, inferSchema=True, header=True)
    return dataframe


df = readCsv("bankcsv.csv")
df.show(10)
df.printSchema()

# Fixing Column names, removing whitespaces
new_column_name_list = list(map(lambda x: x.replace(" ", "_"), df.columns))
df = df.toDF(*new_column_name_list)
df = df.withColumnRenamed("CHQ.NO.", "CHQ_NO")
df.show(10)
df.printSchema()

# drop the column with name dot
df = df.drop(".")
df.show(10)
df.printSchema()

# Adding new TransactionAmount Column
df = df.withColumn('TransactionAmount',
                   when(df['WITHDRAWAL_AMT'].isNull(), df['DEPOSIT_AMT']).when(df['DEPOSIT_AMT'].isNull(),
                                                                               df['WITHDRAWAL_AMT']).otherwise(0))
df.show(10)
df.printSchema()

# Adding new TransactionType Column
df = df.withColumn('TransactionType', when(df.WITHDRAWAL_AMT.isNull(), "CR").when(df.DEPOSIT_AMT.isNull(), "DR"))
df.show(10)
df.printSchema()

# Format the column positions
dfnew = df.select(df.Account_No, df.DATE, df.TRANSACTION_DETAILS, df.CHQ_NO, df.VALUE_DATE, df.WITHDRAWAL_AMT,
                  df.DEPOSIT_AMT, df.TransactionType, df.TransactionAmount, df.BALANCE_AMT)
dfnew.show(10)
dfnew.printSchema()


# write dataframe to csv file
def writeCsv(dataframe, csvfile):
    dataframe.write.mode("overwrite").option("header", "true").csv(csvfile)


writeCsv(dfnew, "updatedbank")

dfupdated = readCsv("updatedbank")
dfupdated.show(10)
dfupdated.printSchema()

# Select all transactions done by Cheque
chequeTransactions = dfupdated.filter(dfupdated.CHQ_NO.isNotNull())
chequeTransactions.show(10)


# Save dataframe to parquet
def writeParquet(dataframe, parquetfile):
    dataframe.write.mode("overwrite").option("header", "true").parquet(parquetfile)


writeParquet(chequeTransactions, "CheckTransactions")

# Selecting all transactions done by CR
crTransactions = dfupdated.filter(dfupdated.TransactionType == "CR")
crTransactions.show(10)

writeParquet(crTransactions, "CrTransactions")

# Selecting all transactions done by DR
drTransactions = dfupdated.filter(dfupdated.TransactionType == "DR")
drTransactions.show(10)

writeParquet(drTransactions, "DrTransactions")

dfupdatednew = readCsv("updatedbank")
dfupdatednew.show(10)
dfupdatednew.printSchema()

# Change DATE column from string to date format
dfupdatednewwithdate = dfupdatednew.withColumn('DATE', to_date(dfupdatednew.DATE, 'yyyy-MM-dd'))
dfupdatednewwithdate.show(10)
dfupdatednewwithdate.printSchema()

# Transactions for a range of date
def transationsInADateRange(datefrom, dateto):
    return dfupdatednewwithdate.filter(dfupdatednewwithdate["DATE"] >= datefrom).filter(dfupdatednewwithdate["DATE"] <= dateto)

# Transactions between to different dates
transationsInADateRange("2017-08-16", "2018-08-16").show(250)

# Transactions on a particular date
transationsInADateRange("2017-08-16", "2017-08-16").show(250)

# Get duplicate transactions
def duplicateTransactions(dataframe):
    dataframe = dataframe.groupBy("Account_No", "DATE", "TransactionType", "TransactionAmount").count().filter("count > 1")
    return dataframe

# Show duplicate transactions
dfduplicatetrancations = duplicateTransactions(dfupdatednewwithdate)
dfduplicatetrancations.show(10)

# Save duplicate transactions to a csv file
writeCsv(dfduplicatetrancations, "dfDuplicateTransactions")

# Drop duplicate transactions in a range of date
dfUpdatedRangeToFindDuplicateTransactions = transationsInADateRange("2017-08-16", "2018-08-16")
dfUpdatedRangeToFindDuplicateTransactions.show(100)
dfUpdatedRangeToFindDuplicateTransactionsRemovedDuplicate = dfUpdatedRangeToFindDuplicateTransactions.dropDuplicates(["Account_No", "DATE", "TransactionType", "TransactionAmount"])
dfUpdatedRangeToFindDuplicateTransactionsRemovedDuplicate.show(100)

# Sum of withdrawal and deposit amount
dfSumOfWithdrawalDeposit = dfUpdatedRangeToFindDuplicateTransactionsRemovedDuplicate.\
    groupBy("Account_No").sum("WITHDRAWAL_AMT", "DEPOSIT_AMT").\
    withColumnRenamed("sum(WITHDRAWAL_AMT)", "Total_Withdrawal").\
    withColumnRenamed("sum(DEPOSIT_AMT)", "Total_Deposit")
dfSumOfWithdrawalDeposit.show(10)

# Change the precision of decimal value
dfSumOfWithdrawalDepositAfterPrecisionSet = dfSumOfWithdrawalDeposit.\
    withColumn("Total_Withdrawal", dfSumOfWithdrawalDeposit.Total_Withdrawal.cast(DecimalType(18, 2))).\
    withColumn("Total_Deposit", dfSumOfWithdrawalDeposit.Total_Deposit.cast(DecimalType(18, 2)))
dfSumOfWithdrawalDepositAfterPrecisionSet.show(10)

# Save the final dataframe to a csv file
writeCsv(dfSumOfWithdrawalDepositAfterPrecisionSet, "SumOfWithdrawalDeposit")
