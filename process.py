from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-sc',"--stock_code", type=str, help="stock code to analyze")
parser.add_argument('-d',"--date", type=str, help="date to analyze")
args = parser.parse_args()

stock_code=str(args.stock_code)
spark = SparkSession.builder.appName("ProcessData").master("spark://spark-master:7077").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.option("multiline","true").json("hdfs://namenode:9000/hadoop/"+ stock_code + "_*.json")

print("top 10 volume day of stock code '" + stock_code + "': \n")
top10df = spark.createDataFrame(df.sort(df.volume.desc()).head(10))
top10df.show(10)
max_volume_date_price_statistic = spark.createDataFrame(top10df.first().price_statistic)

print("price statistic of stock code '"+stock_code+"' in max volume day: \n")
max_volume_date_price_statistic.sort(max_volume_date_price_statistic.volume.desc()).show()

print("transaction of of stock code '"+stock_code+"' in max volume day: \n")
max_volume_date_transaction = spark.createDataFrame(top10df.first().transaction_in_day)
max_volume_date_transaction.show(100)
date=str(args.date)
df_one_date = spark.read.option("multiline","true").json("hdfs://namenode:9000/hadoop/*_"+date+".json")
top10df_in_date = spark.createDataFrame(df_one_date.sort(df_one_date.volume.desc()).head(10))

#Top 10 Co phieu co volume cao nhat trong tung ngay
print("Top 10 stock codes with max volume in day: '"+date+"': \n")
top10df_in_date.show()

#Top 10 Co phieu tang cao nhat trong tung ngay
top10df3 = spark.createDataFrame(df_one_date.sort(df_one_date.change_percent.desc()).head(10))
print("Top 10 stock codes with max percent increase in day: '"+ date +"': \n")
top10df3.show()

#Top 10 Co phieu giam nhieu nhat trong tung ngay
top10df4 = spark.createDataFrame(df_one_date.sort(df_one_date.change_percent.asc()).head(10))
print("Top 10 stock codes with max percent decrease in day: '" + date +"': \n")
top10df4.show()