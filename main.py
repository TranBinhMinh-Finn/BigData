import requests
from bs4 import BeautifulSoup
from datetime import datetime, time, timedelta
import json
from hdfs import InsecureClient
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures
from elasticsearch import Elasticsearch
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-start_date',"--start_date", type=str, help="date start to crawl")
parser.add_argument('-end_date', "--end_date", type=str, help="date end to crawl")
parser.add_argument('-input_stock_code',"--input_stock_code", type=str, help="path of input stock code file in hdfs")
parser.add_argument('-hdfs_client',"--hdfs_client", type=str, help="hdfs client url")
parser.add_argument('-elas_host',"--elas_host", type=str, help="elasticsearch host")
args = parser.parse_args()


start_date = datetime.strptime(str(args.start_date), "%d-%m-%Y")
end_date = datetime.strptime(str(args.end_date), "%d-%m-%Y")
days_delta = (end_date - start_date).days
input_stock_code = str(args.input_stock_code) # /user/app/input_stock_code
hdfs_client_url = str(args.hdfs_client) # http://namenode:9870
elas_host = str(args.elas_host) # node-master:9200


class StockCodeTimeSeries:
    def __init__(
        self,
        name: str,
        date: str,
        reference_price: float,
        max_price: float,
        min_price: float,
        open_price: float,
        close_price: float,
        change: float,
        change_percent: float,
        volume: int,
        price_statistic: list,
        transaction_in_day: list,
    ):

        self.name = name
        self.date = date
        self.reference_price = reference_price
        self.max_price = max_price
        self.min_price = min_price
        self.open_price = open_price
        self.close_price = close_price
        self.change = change
        self.change_percent = change_percent
        self.volume = volume
        self.price_statistic = price_statistic
        self.transaction_in_day = transaction_in_day


class EmptyCrawlData(Exception):

    '''
    raise if crawl data is empty
    '''
    pass


def get_data(code_item: str, date: datetime):
    url = f"https://s.cafef.vn/Lich-su-giao-dich-{code_item}-6.chn?date={date.strftime('%d/%m/%Y')}"
    data = requests.get(url)
    tree = BeautifulSoup(data.content, "html.parser")
    price_list = tree.find(id="price-box").find_all("span", class_="price")
    if not price_list[3].text:
        raise EmptyCrawlData("crawl data is empty")
    change_list = price_list[5].text.split(" ")
    change = float(change_list[0])
    change_percent = float(change_list[1].replace("(", "").replace(")", "").replace("%", ""))


    tblStats_rows = tree.find(id="tblStats").find_all("tr")
    tblStats_rows.pop(0)
    price_statistic = []
    for row in tblStats_rows:
        td_list = row.find_all("td")
        price_statistic.append(
            {"price": float(td_list[0].text), "volume": int(td_list[1].text.replace(",", ""))*10, "percent": float(td_list[2].text.replace("(%)", ""))}
        )

    tblData_rows = tree.find(id="tblData").find_all("tr")
    transaction_in_day = []
    for row in tblData_rows:
        td_list = row.find_all("td")
        transaction_in_day.append({
            "time": td_list[0].text,
            "price": float(td_list[1].text.split(" ")[0]),
            "volume": int(td_list[2].text.replace(",",""))*10,
        })
    return StockCodeTimeSeries(
        date=date.isoformat(),
        name=code_item,
        reference_price=float(price_list[0].text),
        max_price=float(price_list[1].text), 
        min_price=float(price_list[2].text),
        open_price=float(price_list[3].text),
        close_price=float(price_list[4].text),
        change=change,
        change_percent=change_percent,
        volume=int(price_list[6].text.replace(",", ""))*10,
        price_statistic=price_statistic, 
        transaction_in_day=transaction_in_day
    )



client = InsecureClient(hdfs_client_url, user = 'hadoop')
with client.read(input_stock_code, delimiter=",", encoding='utf-8') as reader:
    list_stock_code=[code for code in reader]

es = Elasticsearch(hosts=elas_host)
for day in range(days_delta):
    date = end_date - timedelta(days=day)
    serveral_empty_stock_code = 0
    for stock_code in list_stock_code:
        if serveral_empty_stock_code == 10:
            print(f"break date: {date}")
            break
        try:
            obj = get_data(stock_code, date)
            serveral_empty_stock_code = 0
            # threading.Thread(target=write_hdfs, args=(f"/user/hadoop/crawl_file/{stock_code}_{date.strftime('%d-%m-%Y')}", json.dumps(obj.__dict__))).start()
            client.write(f"/hadoop/crawl_file/{stock_code}_{date.strftime('%d-%m-%Y')}.json", data = f"{json.dumps(obj.__dict__)}\n", encoding = 'utf-8', overwrite=True)
            es.update(index="chungkhoan", id=f"{stock_code}_{date.strftime('%d-%m-%Y')}", doc=json.loads(json.dumps(obj.__dict__)), doc_as_upsert=True)
        except EmptyCrawlData as e:
            serveral_empty_stock_code += 1
            print(f"error: {e}, {stock_code} {date}")
        except Exception as e:
            print(f"error: {e}, {stock_code} {date}")