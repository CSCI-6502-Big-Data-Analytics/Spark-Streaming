from time import sleep
from json import dumps
from kafka import KafkaProducer, KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic
from csv import DictReader

topic_name = "creditcard"
topic_list = [
    NewTopic(
        name=topic_name,
        num_partitions=1,
        replication_factor=1,
        topic_configs={'retention.ms': '300000'}
    )
]

# Retrieving already-created list of topics and then deleting

client = KafkaClient(bootstrap_servers=['localhost:9092'])
metadata = client.cluster
future = client.cluster.request_update()
client.poll(future=future)
broker_topics = metadata.topics()

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
if topic_name in broker_topics:
    deletion = admin_client.delete_topics([topic_name])
    sleep(3)
    try:
        future = client.cluster.request_update()
        client.poll(future=future)
    except KafkaError as e:
        print(e)
        pass
#admin_client.create_topics(new_topics=topic_list, validate_only=False)


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
						 
with open('data/creditcard.csv', 'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
		producer.send('creditcard', value=row)
		sleep(3)    
	
