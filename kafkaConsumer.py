import time
import json
import threading
import logging
import sys
from kafka import KafkaConsumer
import pandas as pd
from s3fs import S3FileSystem

kafka_bootstrap_servers=['54.193.21.143:9092']

kafka_topic="demo_testing1"

consumer=KafkaConsumer(kafka_topic,bootstrap_servers=kafka_bootstrap_servers,value_deserializer=lambda v:json.loads(v.decode('utf-8')))

# consumer will take the data from the topic and load it to wherever we want


# we want to upload data from consumer to s3 bucket so import a library
s3=S3FileSystem()
# for each row create a file and store in s3 bucket, also assign each file unique name

for count,i in enumerate(consumer):
    with s3.open("s3://kafka-stock-market-bucket-1/stock_market_{}.json".format(count),'w') as file:
        json.dump(i.value,file)
