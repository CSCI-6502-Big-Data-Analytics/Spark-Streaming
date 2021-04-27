from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import pymongo
import uuid
import random

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["spark"]
collection = db["resultData"]


def printMessage(message):
  for st in message.collect():
    input_dict = json.loads(st[1])
    id = uuid.uuid4()
    input_dict["transaction_id"] = str(id)
    print(input_dict)
    result_dict = { "transaction_id": str(id), "result": random.randint(0,1) }
    collection.insert_one(result_dict)

sc = SparkContext(appName="KafkaStream")
ssc = StreamingContext(sc,10)

message = KafkaUtils.createDirectStream(ssc,topics=["creditcard"],kafkaParams={"metadata.broker.list":"localhost:9092"})
message.foreachRDD(printMessage)

ssc.start()
ssc.awaitTermination()



# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *


# # Spark session & context
# spark = (SparkSession
#          .builder
#          .master('local')
#          .appName('fraud-detection')
#          # Add kafka package
#          .config("spark.jars", "./spark-sql-kafka-0-10_2.12-3.1.1.jar, ./kafka-clients-2.8.0.jar")
#          .config("spark.executor.extraClassPath", "./spark-sql-kafka-0-10_2.12-3.1.1.jar, ./kafka-clients-2.8.0.jar")
#          .config("spark.executor.extraLibrary", "./spark-sql-kafka-0-10_2.12-3.1.1.jar, ./kafka-clients-2.8.0.jar")
#          .config("spark.driver.extraClassPath", "./spark-sql-kafka-0-10_2.12-3.1.1.jar, ./kafka-clients-2.8.0.jar")
#          .getOrCreate())
# sc = spark.sparkContext


# mySchema = StructType()\
#  .add("Time", IntegerType())\
#  .add("V1", StringType())\
#  .add("V2", StringType())\
#  .add("V3", StringType())\
#  .add("V4", StringType())\
#  .add("V5", StringType())\
#  .add("V6", StringType())\
#  .add("V7", StringType())\
#  .add("V8", StringType())\
#  .add("V9", StringType())\
#  .add("V10", StringType())\
#  .add("V11", StringType())\
#  .add("V12", StringType())\
#  .add("V13", StringType())\
#  .add("V14", StringType())\
#  .add("V15", StringType())\
#  .add("V16", StringType())\
#  .add("V17", StringType())\
#  .add("V18", StringType())\
#  .add("V19", StringType())\
#  .add("V20", StringType())\
#  .add("V21", StringType())\
#  .add("V22", StringType())\
#  .add("V23", StringType())\
#  .add("V24", StringType())\
#  .add("V25", StringType())\
#  .add("V26", StringType())\
#  .add("V27", StringType())\
#  .add("V28", StringType())\
#  .add("Amount", DoubleType())\
#  .add("Class", IntegerType())\

# df = (spark
#   .readStream
#   .format("kafka")
#   .option("kafka.bootstrap.servers", "localhost:9092") # kafka server
#   .option("subscribe", "creditcard") # topic
#   .load())
# # df1 = df.selectExpr("CAST(value AS STRING)")


# df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")\
#     .select(from_json(col("value"), mySchema).alias("data"), "timestamp")\
#     .select("data.*", "timestamp")

# df1.printSchema()

# df1.writeStream\
#     .trigger(processingTime="5 seconds")\
#     .outputMode("update")\
#     .format("console")\
#     .start()\
#     .awaitTermination()

# list_persons = map(lambda row: row.asDict(), df1.collect())

# print(list_persons)
