from kafka import KafkaConsumer
from threading import Thread
from server import number_of_users
import pg8000
import json

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
	                   orientation=column
                   )
                   DISTRIBUTED BY (date)
                ''') 
cursor.execute(f'TRUNCATE TABLE {raw_table}')
conn.commit()
try:
  for message in consumer:
    msg = json.loads(message.value.decode('cp1251'))
    print(f"{msg['id']}, {msg['date']}, {msg['channel_id']}, {msg['action']}")
    #cursor.execute(f'''INSERT INTO {raw_table} (id,date,channel_id,action) values({msg["id"]}, '{msg["date"]}', {msg["channel_id"]}, {msg["action"]});''')
    #conn.commit()
finally:
  conn.close()
