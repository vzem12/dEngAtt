from kafka import KafkaConsumer
from threading import Thread
import pg8000
import json
from pyspark.sql import SparkSession, Row
from datetime import datetime as dt
import time

f = '%Y-%m-%d %H:%M:%S'

number_of_users = 999

consumer = KafkaConsumer(bootstrap_servers='vm-strmng-s-1.test.local:9092',
                         group_id='my_group')

topics = list()

for id in range(number_of_users): 
  topics.append(f'iptv_user_{id+1}')
  
consumer.subscribe(topics)

conn = pg8000.connect(database='postgres',
                      host='192.168.77.21', 
                      port=5432, 
                      user='zemtsov', 
                      password='854ss')
cursor = conn.cursor()
                                      
raw_table = 'zemtsov_raw_data'

cursor.execute(f'''CREATE TABLE IF NOT EXISTS {raw_table} (
                     id bigint,
                     date timestamp,
                     channel_id integer,
                     action boolean)
                   WITH (
                     appendonly=true,
                     compresstype=zlib,
                     compresslevel=1,
                     orientation=column)
                   DISTRIBUTED BY (date);
                ''') 
cursor.execute(f'TRUNCATE TABLE {raw_table}')
conn.commit()

def toStorage(q):
  spark = SparkSession.builder.getOrCreate()
  lstTs = time.time()
  rows = []
  while clientRunning:
    msg = q.get()
    rows.append(msg['id'],
                dt.strptime(msg['date'],f),
                msg['channel_id'],
                msg['action'])
    if (time.time() - lstTs) >= interval and len(rows) != 0:
      df = spark.createDataFrame(rows,schema='id long, date timestamp, channel_id int, action boolean')
      df.write.mode('append').parquet(f'hdfs://vm-dlake2-m-1.test.local/user/zemtsov/data/{raw_table}.parquet')
      rows=[]
    lstTs = time.time()
  spark.stop()

def toGP(q):
  while clientRunning:
    msg = q.get()
    cursor.execute(f'''INSERT INTO {raw_table} (id,date,channel_id,action) values({msg["id"]}, '{msg["date"]}'$
    conn.commit()
  
try:
  qStorage = Queue()
  qGP = Queue()
  storage_thread = Thread(target=toStorage,args=(qStorage,))
  gp_thread = Thread(target=toGP,args=(qGP,))
  storage_thread.start()
  gp_thread.start()
  
  for message in consumer:
    msg = json.loads(message.value.decode('cp1251'))
    #print(f"{msg['id']}, {msg['date']}, {msg['channel_id']}, {msg['action']}")
    qStorage.put(msg)
    qGP.put(msg)

finally:
  conn.close()
