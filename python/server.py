from userGen import UserInfo
from kafka import KafkaProducer
from threading import Thread
from datetime import datetime as dt
import datetime
import pg8000
import time

number_of_users = 999
users = list()
counter = 0

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
  if channel[2] == 'Федеральные': chan_category['Федеральные'].append(channel[0])
  if channel[2] == 'Спорт': chan_category['Спорт'].append(channel[0])
  if channel[2] == 'Кино': chan_category['Кино'].append(channel[0])
  if channel[2] == 'Познавательные': chan_category['Познавательные'].append(channel[0])
  if channel[2] == 'Музыка': chan_category['Музыка'].append(channel[0])
  if channel[2] == 'Развлечения': chan_category['Развлечения'].append(channel[0])
  
channel_difs = [
  [18,25,
    [chan_category['Федеральные'], 
    chan_category['Спорт'],
    chan_category['Кино'],
    chan_category['Познавательные'],
    chan_category['Музыка'],
    chan_category['Развлечения']],
    [10,30,30,20,40,50]],
  [25,35,
    [chan_category['Федеральные'], 
    chan_category['Спорт'],
    chan_category['Кино'],
    chan_category['Познавательные'],
    chan_category['Музыка'],
    chan_category['Развлечения']],
    [20,30,20,40,10,15]],
  [35,50,
    [chan_category['Федеральные'], 
    chan_category['Спорт'],
    chan_category['Кино'],
    chan_category['Познавательные'],
    chan_category['Музыка'],
    chan_category['Развлечения']],
    [40,30,20,10,5,2]],
  [50,70,
    [chan_category['Федеральные'], 
    chan_category['Спорт'],
    chan_category['Кино'],
    chan_category['Познавательные'],
    chan_category['Музыка'],
    chan_category['Развлечения']],
    [60,30,10,5,1,1]],
  [70,90,
    [chan_category['Федеральные'], 
    chan_category['Спорт'],
    chan_category['Кино'],
    chan_category['Познавательные'],
    chan_category['Музыка'],
    chan_category['Развлечения']],
    [70,10,5,1,0,0]]
]

def send_topic(queue,user):
  global counter
  while status_gen:
    if dt.strptime(user.date,user.f) >= dt.now() and not user.state: break
    counter += 1
    msg = user.userGen()
    producer.send(queue,msg)
    time.sleep(0.01)

status_gen = True

try:
  for id in range(number_of_users):
    queue = f'iptv_user_{id+1}'
    user = UserInfo(id+1)
    conn.rollback()
    cursor.execute(f"select birth_date from zemtsov_names_dict where id={id+1};")
    birthday = cursor.fetchone()[0]
    today = dt.now()
    age = today.year - birthday.year - ((today.month,today.day)<(birthday.month,birthday.day))
    for dif in channel_difs:
      if age in range(dif[0],dif[1]): 
        user.channels_dif = [dif[2],dif[3]]
        break
    #print(channels_dif)
    users.append(Thread(target=send_topic, args=(queue,user)))

  try:
    for user in users:
      user.start()
  except:
    ststus_gen = False
    for user in users:
      user.join()
  finally:
    ststus_gen = False
    for user in users:
      user.join()
except KeyboardInterrupt:
  ststus_gen = False
  try:
    for user in users:
      user.join()
  except:
    pass
finally:
  print(counter)
  producer.close()
  conn.close()
