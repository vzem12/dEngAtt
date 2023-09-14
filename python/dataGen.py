# /bin/bash python3

from kafka import KafkaProducer
import datetime
import random
import json

counter = 0
f = '%d.%m.%Y %H:%M:%S'

class UserInfo():
  watch_id = 0
  channel_id = 1

  def __init__(self,id):
    self.id = id
    
  def infoGen(self, date, action):
    watch_id = self.id * 10000000 + self.watch_id
    msg = {'id': watch_id,
            'date': date,
            'channel_id': self.channel_id,
            'action': action}
    #self.watch_id += 1
    return json.dumps(msg).encode('cp1251')

def getLastDate(last, delta):
  rnd_seconds = random.randrange(delta)
  return (datetime.datetime.strptime(last, f) + datetime.timedelta(seconds=rnd_seconds)).strftime(f)

def userThread(id):
  global counter
  
  user = UserInfo(id)
  date = '01.01.2022 00:00:00'
  date = getLastDate(date, 7200)
  state = False
 
  while True:
    if datetime.datetime.strptime(date,f) >= datetime.datetime.now() and not state: break
    counter += 1
    if not state:
      user.watch_id += 1
      user.channel_id = random.randint(1,30)
      delta = 7200
    else:
      delta = random.randint(1,random.choices([1,5,20],[172800, 28800, 5])[0]) #Выключение на 2 дня (от 0 до 2-х), на 8 часов, переключение каналов
      
    msg = user.infoGen(date, not state)
    date = getLastDate(date, delta)
    state = not state 
    print(msg, counter)

if __name__ == '__main__':
  scriptState = True
  try:
    for id in range(200):
      userThread(id+1)
  except:
    print(counter)
    

