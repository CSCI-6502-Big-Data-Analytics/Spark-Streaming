from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import pymongo
import random
import os, sys
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql.types import FloatType
import pyspark.sql.functions as f
import threading
import uuid

spark = SparkSession.builder.appName('fraud-detection').master("local[*]").getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 10)

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["spark"]
collection = db["resultData"]

LR_MODEL_SAVEPATH = 'saves/LRBalancedModel'
PIPELINE_SAVEPATH = 'saves/pipelineModelBalanced'

pipelineModel = PipelineModel.load(PIPELINE_SAVEPATH)
lrModel = LogisticRegressionModel.load(LR_MODEL_SAVEPATH)

def addToMongo(data):
    collection.insert_one(data)
    return

def predict(input_transaction_list, pipelineModel, lrModel, id_list):
    
    testDf = spark.createDataFrame([Row(**i) for i in input_transaction_list])
    for col in testDf.columns:
        testDf = testDf.withColumn(col, testDf[col].cast(FloatType()))
    all_input_cols = testDf.columns
    testDf = pipelineModel.transform(testDf)
    selectedCols = ['features'] + all_input_cols 
    testDf = testDf.select(selectedCols)    
       
    outputDf = lrModel.transform(testDf)
    predictions = outputDf.select(f.collect_list('prediction')).first()[0]
    print("\n=========================================================================\n")
    for i in range(len(predictions)):
      print(id_list[i], " : ", int(predictions[i]))
      result_dict = { "transaction_id": str(id_list[i]), "result": int(predictions[i]) }
      threading.Thread(target=addToMongo, args=(result_dict,)).start()
    print("\n=========================================================================\n")

def formatTransaction(transaction):
  input_transaction_list, id_list = [], []
  for st in transaction.collect():
    transaction_dict = json.loads(st[1])
    #transaction_dict["_c0"] = random.randint(0,1000)
    transaction_dict["Time"] = int(transaction_dict["Time"])
    transaction_dict["Amount"] = float(transaction_dict["Amount"])
    id_list.append(transaction_dict["transaction_id"])
    del transaction_dict["Class"], transaction_dict["transaction_id"]
    input_transaction_list.append(transaction_dict)
    
  if input_transaction_list:
    predict(input_transaction_list, pipelineModel, lrModel, id_list)
  

transactions = KafkaUtils.createDirectStream(ssc, topics=["creditcard"], kafkaParams={"metadata.broker.list":"localhost:9092"})
if transactions:
  transactions.foreachRDD(formatTransaction)

ssc.start()
ssc.awaitTermination()
