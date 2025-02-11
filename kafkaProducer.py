import time
import json
import threading
import logging
import sys
from kafka import KafkaProducer
import pandas as pd
from time import sleep
from json import dumps
from polygon import RESTClient

kafka_bootstrap_servers=['54.193.21.143:9092']
kafka_topic="demo_testing1"


client = RESTClient(api_key="uVqJ5r6lw_0bQ81E1RRlInvtWFMLSD5_")

# Request data using client methods.
ticker="AAPL"
aggs=[]
for a in client.list_aggs(ticker=ticker, multiplier=1, timespan="minute", from_="2023-01-01", to="2023-01-13", limit=5000):
    aggs.append(a)
    print(a)
    print(" ")

# print(aggs)


# json message about the weather of city will be written to kafka topic 
# producer is something thaat produces data (sensors,hospitals, advertising , news) and sent ot kafka server
# create a producer object as we want to public data to topic
producer=KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,value_serializer=lambda v:json.dumps(v).encode('utf-8'))
# make sure to sned data in json format
for i in range(10):
    producer.send(kafka_topic,value=f"{i,i}").get(timeout=2)

df=pd.read_csv("indexProcessed.csv")
# get data  in random orde
# to_dict to convert to json format
print(df.sample(1).to_dict(orient="records"))

i=100
while(True):
    data=df.sample(1).to_dict(orient="records")[0]
    producer.send(kafka_topic,value=data).get(timeout=5)
    time.sleep(2)

producer.flush()
# now performa the operations on df
    