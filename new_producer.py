import flask
import json
from flask import request, abort, Response
import pymongo
from time import sleep
from json import dumps
from kafka import KafkaProducer, KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic
from csv import DictReader
import threading
import uuid

app = flask.Flask(__name__)
app.config["DEBUG"] = True

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["spark"]
collection = db["transactionData"]

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
    sleep(2)
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

def addToMongo(data):
    collection.insert_one(data)
    return

@app.route('/predict', methods=['POST'])
def predictApi():
    transactionData = request.json
    transactionData["transaction_id"] = str(uuid.uuid4())
    producer.send('creditcard', value=transactionData)
    threading.Thread(target=addToMongo, args=(transactionData,)).start()
    # for thread in threading.enumerate():
    #     print(thread.name)
    return {
        'success': True,
        'message': "Transaction sent to the Spark service",
        'transactionId': transactionData["transaction_id"]
    }    

@app.route('/predictionResult', methods=['GET'])
def getPrediction():    
    transactionId = request.args.get('transactionId')
    mongoDocs = list(db["resultData"].find({"transaction_id": transactionId}))
    print(mongoDocs)
    mongoDocsNew = []
    for val in mongoDocs:
        del val['_id']
        mongoDocsNew.append(val)
    print(mongoDocsNew)
    return Response(json.dumps(mongoDocsNew),  mimetype='application/json')

app.run()