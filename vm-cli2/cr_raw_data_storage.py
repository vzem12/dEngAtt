from fastavro import writer

schema = {
  'doc': 'zemtsov_raw_data',
  'name': 'zemtsov_raw_data',
  'namespace': 'zemtsov',
  'type': 'record',
  'fields': [
    {'name': 'id', 'type': 'long'},
    {'name': 'date', 'type': 'long'},
    {'name': 'channel_id', 'type': 'int'},
    {'name': 'action', 'type': 'boolean'},
  ],
}

with open('weather.avro', 'wb') as out:
    writer(out, schema)
