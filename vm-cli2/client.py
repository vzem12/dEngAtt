#! /bin/python3

from kafka import KafkaConsumer
from threading import Thread
import json
from pyspark.sql import SparkSession
from datetime import datetime as dt
import time
from queue import Queue

f = '%Y-%m-%d %H:%M:%S'
clientRunning = True

consumer = KafkaConsumer(bootstrap_servers='vm-strmng-s-1.test.local:9092',
                         group_id='zemtsov_iptv_users')
consumer.subscribe('zemtsov_iptv_user')                                   

raw_table = 'zemtsov_raw_data'

def toStorage(q):
  spark = SparkSession.builder.getOrCreate()
  interval = 30  
  lstTs = time.time()
  rows = []
  while clientRunning:
    msg = None
    try:
      msg = q.get(timeout=10)
    except:
      pass
    if msg is not None:
      rows.append((msg['id'],
                  dt.strptime(msg['date'],f),
                  msg['channel_id'],
                  msg['action']))
    if (((time.time() - lstTs) >= interval) or len(rows)>=100000) and (len(rows) != 0):
      print('Data Store Start')
      df = spark.createDataFrame(rows,schema='id long,date timestamp,channel_id int,action boolean')
      df.write.mode('append').parquet(f'hdfs://vm-dlake2-m-1.test.local/user/zemtsov/data/{raw_table}.parquet')
      rows=[]
      print(f'Data Stored at {dt.now().strftime(f)}')
      lstTs = time.time()
  spark.stop()
  
try:
  qStorage = Queue()
  storage_thread = Thread(target=toStorage,args=(qStorage,))
  storage_thread.start()
  
  for message in consumer:
    msg = json.loads(message.value.decode('cp1251'))
    #print(f"{msg['id']}, {msg['date']}, {msg['channel_id']}, {msg['action']}")
    qStorage.put(msg)

finally:
  clientRunning = False
