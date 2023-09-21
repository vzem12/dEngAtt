import clickhouse_connect
import csv
import random
from datetime import datetime as dt
from datetime import timedelta as td
import datetime



conn = clickhouse_connect.get_client(database='destudy',
                                     host='vm-widest-m-1.test.local', 
                                     port=8123, 
                                     username='zemtsov', 
                                     password='854ss')
names_table = 'zemtsov_names_dict'
channels_table = 'zemtsov_chan_dict'
                                      
def create_tables():
  commands = list()
  commands.append(f'''
    CREATE TABLE IF NOT EXISTS {names_table} (
      id Int8,
      name String,
      surname String,
      birth_date Date,
      gender String)
    ENGINE = MergeTree()
    ORDER BY id;
  ''')
  commands.append(f'TRUNCATE TABLE {names_table};')
  commands.append(f'''
    CREATE TABLE IF NOT EXISTS {channels_table} (
      id Int8,
      name String,
      category String)
    ENGINE = MergeTree()
    ORDER BY id;
  ''')
  commands.append(f'TRUNCATE TABLE {channels_table};')
  
  for command in commands:  
    conn.command(command)
    
def _genFSurname(surname):
  end = surname[-2:]
  if end in ['ов', 'ин', 'ев', 'ив', 'ын', 'ёв']: surname += 'а'
  if end in ['ий','ой','ый']: surname = surname[:-2] + 'ая'
  return surname
  
def _getGender(age):
  if age in range(18,30): gender = random.choices(['male','female'],[51,49])[0]
  if age in range(30,40): gender = random.choices(['male','female'],[50,50])[0]
  if age in range(40,50): gender = random.choices(['male','female'],[48,52])[0]
  if age in range(50,60): gender = random.choices(['male','female'],[45,55])[0]
  if age in range(60,70): gender = random.choices(['male','female'],[40,60])[0]
  if age in range(70,80): gender = random.choices(['male','female'],[30,70])[0]
  if age in range(80,90): gender = random.choices(['male','female'],[20,80])[0]
  return gender
    
def filling_dicts():
  try:
    create_tables()
    with open('channels.csv', 'r') as f:
      ch = csv.reader(f,delimiter=',')
      for channel in ch:
        conn.command(f"INSERT INTO {channels_table} (id,name,category) values({channel[0]},'{channel[1]}','{channel[2]}');")

    with open('surnames.txt', 'r') as f:
      surnames = list()
      for line in f:
        surnames.append(line.strip())
        
    with open('names_male.txt', 'r') as f:
      names_male = list()
      for line in f:
        names_male.append(line.strip())
        
    with open('names_female.txt', 'r') as f:
      names_female = list()
      for line in f:
        names_female.append(line.strip())
        
    ages = [
            [x for x in range(18,25)],
            [x for x in range(25,30)],
            [x for x in range(30,35)],
            [x for x in range(35,40)],
            [x for x in range(40,45)],
            [x for x in range(45,50)],
            [x for x in range(50,55)],
            [x for x in range(55,60)],
            [x for x in range(60,65)],
            [x for x in range(65,70)],
            [x for x in range(70,75)],
            [x for x in range(75,80)],
            [x for x in range(80,90)]
    ]
    ages_w = [6,8,8,8,7,6,7,8,6,5,4,3,1]

    for id in range(999):
      f = '%Y-%m-%d'
      age = random.choice(random.choices(ages,ages_w)[0])
      birth_date = ((dt.now() - td(days=(age*365)-(age//4))) + td(days=random.randint(0,360))).strftime(f)
      gender = _getGender(age)
      if gender == 'male':
        name = random.choice(names_male)
        surname = random.choice(surnames)
      elif gender == 'female':
        name = random.choice(names_female)
        surname = _genFSurname(random.choice(surnames))
      
      conn.command(f'''INSERT INTO {names_table} 
                         (id,name,surname,birth_date,gender)
                         values ({id+1},'{name}','{surname}','{birth_date}','{gender}');
                      ''')
  finally:
    conn.close()
    
if __name__ == '__main__':
  filling_dicts()
