FROM python:3.8
 
WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

ENTRYPOINT ["python"]
CMD ["main.py",  "--start_date", "1-1-2022" , "--end_date", "1-2-2022", "--input_stock_code", "input_stock_code.txt", "--hdfs_client", "http://namenode:9870", "--elas_host", "http://elasticsearch:9200"]