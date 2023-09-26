#! /bin/python3

from userGen import UserInfo
from kafka import KafkaProducer
from datetime import datetime as dt
import datetime
import pg8000
import time
import random

number_of_users = 2000
counter = 0

producer = KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092')

conn = pg8000.connect(database='postgres',
                      host='192.168.77.21', 
                      port=5432, 
                      user='zemtsov', 
                      password='854ss')
cursor = conn.cursor()

conn.rollback()
cursor.execute(f"select * from zemtsov_chan_dict;")
channels = cursor.fetchall()
chan_category = {'Федеральные':[],
                 'Спорт':[],
                 'Кино':[],
                 'Познавательные':[],
                 'Музыка':[],
                 'Развлечения':[]}
for channel in channels:
  chan_category[channel[2]].append(channel[0])
  
channel_difs = [
  [18,25,'male',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]], 
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [10,40,20,15,20,60]],
  [18,25,'female',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]],
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [5,10,40,15,40,50]],
  [25,35,'male',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]],
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [20,30,20,40,10,30]],
  [25,35,'female',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]],
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],   
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [15,5,40,30,15,20]],
  [35,50,'male',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]],
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [40,30,20,10,5,2]],
  [35,50,'female',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]],
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [35,5,40,10,2,8]],
  [50,70,'male',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]],
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [60,40,20,5,0,3]],
  [50,70,'female',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]],
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [25,3,50,5,0,10]],
  [70,90,'male',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]],
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [70,10,5,1,0,0]],
  [70,90,'female',
    [[chan_category['Федеральные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Федеральные']))]],
    [chan_category['Спорт'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Спорт']))]],
    [chan_category['Кино'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Кино']))]],
    [chan_category['Познавательные'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Познавательные']))]],
    [chan_category['Музыка'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Музыка']))]],
    [chan_category['Развлечения'],[random.choice([2,10,30,50,70]) for _ in range(len(chan_category['Развлечения']))]]],
    [70,30,0,0,0,8]],
]

def send_topic(queue,user,birthday,gender):
  today = dt.now()
  age = today.year - birthday.year - ((today.month,today.day)<(birthday.month,birthday.day))
  for dif in channel_difs:
    if age in range(dif[0],dif[1]) and gender == dif[2]:
      user.channels_dif = [dif[3],dif[4]]
      break
  #print(f'User {user.id} started')

  while status_gen:
    if dt.strptime(user.date,user.f) >= dt.now() and not user.state: break
    msg = user.userGen()
    producer.send(queue,msg)
    #time.sleep(0.01)
  print(f'User {user.id} has generated')
    
status_gen = True

conn.rollback()
cursor.execute(f"select birth_date, gender from zemtsov_names_dict;")
users_info = dict()
results = cursor.fetchall()
i = 1
for res in results:
  users_info[i] = {
    'birthday':res[0],
    'gender':res[1]
  }
  i += 1

conn.close()

try:
  queue = f'zemtsov_iptv_user'
  for id in range(number_of_users):
    user = UserInfo(id+1)
    #print(channels_dif)
    send_topic(queue,user,users_info[id+1]['birthday'],users_info[id+1]['gender'])  
  
  print('Finish!') 
  status_gen = False
    
except KeyboardInterrupt:
  status_gen = False

finally:
  status_gen = False
  print(counter)
  producer.close()
