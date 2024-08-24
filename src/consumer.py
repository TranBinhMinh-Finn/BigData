from array import ArrayType
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
import config, io_cluster
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from hdfs import InsecureClient
import json
from datetime import datetime

schema = StructType([
      StructField("date",StringType(),True),
      StructField("name",StringType(),True),
      StructField("reference_price",FloatType(),True),
      StructField("max_price",FloatType(),True),
      StructField("min_price",FloatType(),True),
      StructField("open_price",FloatType(),True),
      StructField("close_price",FloatType(),True),
      StructField("change",FloatType(),True),
      StructField("change_percent",FloatType(),True),
      StructField("volume",IntegerType(),True),
      StructField("price_statistic",ArrayType(StructType([
        StructField("price", FloatType()),
        StructField("volume", IntegerType()),
        StructField("percent", FloatType())
      ])),True),
      StructField("transaction_in_day",ArrayType(StructType([
        StructField("price", FloatType()),
        StructField("volume", IntegerType()),
        StructField("time", StringType())
      ])),True),
  ])

if __name__ == "__main__":
    
    APP_NAME="KafkaConsumer"

    app_config = config.Config(elasticsearch_host="elasticsearch",
                               elasticsearch_port="9200",
                               elasticsearch_input_json="yes",
                               elasticsearch_nodes_wan_only="false",
                               hdfs_namenode="hdfs://namenode:9000"
                               )
    spark = app_config.initialize_spark_session(APP_NAME)
    sc = spark.sparkContext

    client = InsecureClient('http://namenode:9870', user = 'hadoop')
    es = Elasticsearch(hosts='http://elasticsearch:9200')
    consumer = KafkaConsumer(
        'stock-data',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for event in consumer:
        event_data = event.value
        df = spark.createDataFrame([event_data], schema)
        df.show(1)
        df_date = df.first().date[:df.first().date.find('T')]
        client.write(f"/hadoop/{df.first().name}_{df_date}.json", data = f"{json.dumps(event_data)}\n", encoding = 'utf-8', overwrite=True)
        es.update(index="stocks", id=f"{df.first().name}_{df.first().date}", doc=json.loads(json.dumps(event_data)), doc_as_upsert=True)
