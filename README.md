<div align="center">

# Stocks Data Storing and Analyzing
In this project, we want to get some insight of the labor market in a certain period of time from crawled data of a website.
Crawled data is first uploaded to HDFS cluster. At the same time, this data is uploaded to the elasticsearch cluster (for visualization in Kibana).

# Steps
Start the docker app:
```
docker-compose up -d
```

Copy the stock codes file to the namenode:
```
docker cp .\input_stock_code.txt namenode:/home/
```

Attach shell to the hdfs namenode:
```
docker-compose exec -it namenode bash
```
then create directories in the cluster:
```
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/hadoop
```

Start the crawler app:
```
docker-compose up -d app
```

Copy the file processing python job to Spark cluster:
```
docker cp .\process.py spark-master:/home/
```

Attach shell to the spark-master node:
```
docker-compose exec -it spark-master bash
```
then execute the job copied into the cluster:
```
spark/bin/spark-submit --master spark://spark-master:7077 -sc <stock code> -d <dd-mm-YYYY>
```

# Management pages

Hadoop:
```
http://localhost:9870
```

Spark:
```
http://localhost:8080
```

Kibana:
```
http://localhost:5601
```