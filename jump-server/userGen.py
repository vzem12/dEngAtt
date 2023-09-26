import datetime
import random
import json

class UserInfo():
  f = '%Y-%m-%d %H:%M:%S'
  watch_id = 0
  channel_id = 1
  date = '2021-01-01 00:00:00'
  state = False
  channels_dif = None

  def __init__(self,id):
    self.id = id
    
  def infoGen(self, date, action):
    watch_id = self.id * 10000000 + self.watch_id
    msg = {'id': watch_id,
           'date': date,
           'channel_id': self.channel_id,
           'action': action}
    return json.dumps(msg).encode('cp1251')

  def getNextDate(self, last, delta):
    rnd_seconds = random.randrange(delta)
    return (datetime.datetime.strptime(last, self.f) +\
            datetime.timedelta(seconds=rnd_seconds)).strftime(self.f)

  def userGen(self):
    if self.watch_id == 0: self.date = self.getNextDate(self.date, 259200)
    if not self.state:
      self.watch_id += 1
      try:
        channel_dif_group = random.choices(self.channels_dif[0],self.channels_dif[1])[0]
        self.channel_id = random.choices(channel_dif_group[0],channel_dif_group[1])[0]
      except Exception as err:
         print(f' Error userGen: channels_dif - {err}')
      delta = 7200
    else:
      hour = datetime.datetime.strptime(self.date,self.f).hour
      if hour in [23,0,1,2,3,4,5,6,7]:
        weights = [5,10,1]
      elif hour in [8,9,10]:
        weights = [1,10,20]
      elif hour in [11,12,13,14,15,16]:
        weights = [5,10,1]
      else:
        weights = [1,10,20]
        
      #Выключение на 5 days (от 0 до 5), 8 часов, переключение каналов
      delta = random.randint(1,random.choices([432000, 28800, 5],weights)[0])
    
    self.state = not self.state
    msg = self.infoGen(self.date, self.state)
    self.date = self.getNextDate(self.date, delta)
    #print(msg) 
    return msg

if __name__ == '__main__':
  pass
