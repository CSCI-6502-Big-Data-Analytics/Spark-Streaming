from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'creditcard',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     value_deserializer=lambda x: loads(x.decode('ascii')))
	 
for message in consumer:
	message = message.value
	print('{}'.format(message))
	print('------------------------------')