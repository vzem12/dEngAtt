from fastavro import writer
from hdfs.client import Client

client = Client("hdfs://vm-dlake2-m-1:8020/",root="/user/zemtsov",timeout=10000,session=False)

if not client.exists('/user/zemtsov/data/zemtsov_raw_data.avro'):
  schema = {
    'doc': 'zemtsov_raw_data',
    'name': 'zemtsov_raw_data',
    'namespace': 'zemtsov',
    'type': 'record',
    'fields': [
      {'name': 'id', 'type': 'long'},
      {'name': 'date', 'type': 'long', 'logicalType': 'timestamp-millis'},
      {'name': 'channel_id', 'type': 'int'},
      {'name': 'action', 'type': 'boolean'},
    ],
  }
  
  with open('zemtsov_raw_data.avro', 'wb') as out:
      writer(out, schema)
  if not client.exists('/user/zemtsov/data'):
    client.makedirs('/user/zemtsov/data')
  client.upload('data/zemtsov_raw_data.avro', 'zemtsov_raw_data.avro', cleanup=True)
