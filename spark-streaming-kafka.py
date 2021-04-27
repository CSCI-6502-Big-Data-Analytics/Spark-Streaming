# from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils

# sc = SparkContext(appName="KafkaStream")
# ssc = StreamingContext(sc,10)

# message = KafkaUtils.createDirectStream(ssc,topics=["creditcard"],kafkaParams={"metadata.broker.list":"localhost:9092"})
# message.pprint()

# ssc.start()
# ssc.awaitTermination()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Spark session & context
spark = (SparkSession
         .builder
         .master('local')
         .appName('fraud-detection')
         # Add kafka package
         .config("spark.jars", "./spark-sql-kafka-0-10_2.12-3.1.1.jar, ./kafka-clients-2.8.0.jar")
         .config("spark.executor.extraClassPath", "./spark-sql-kafka-0-10_2.12-3.1.1.jar, ./kafka-clients-2.8.0.jar")
         .config("spark.executor.extraLibrary", "./spark-sql-kafka-0-10_2.12-3.1.1.jar, ./kafka-clients-2.8.0.jar")
         .config("spark.driver.extraClassPath", "./spark-sql-kafka-0-10_2.12-3.1.1.jar, ./kafka-clients-2.8.0.jar")
         .getOrCreate())
sc = spark.sparkContext

df = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092") # kafka server
  .option("subscribe", "creditcard") # topic
  .load())
# df1 = df.selectExpr("CAST(value AS STRING)")

mySchema = StructType()\
 .add("Time", IntegerType())\
 .add("V1", DoubleType())\
 .add("V2", DoubleType())\
 .add("V3", DoubleType())\
 .add("V4", DoubleType())\
 .add("V5", DoubleType())\
 .add("V6", DoubleType())\
 .add("V7", DoubleType())\
 .add("V8", DoubleType())\
 .add("V9", DoubleType())\
 .add("V10", DoubleType())\
 .add("V11", DoubleType())\
 .add("V12", DoubleType())\
 .add("V13", DoubleType())\
 .add("V14", DoubleType())\
 .add("V15", DoubleType())\
 .add("V16", DoubleType())\
 .add("V17", DoubleType())\
 .add("V18", DoubleType())\
 .add("V19", DoubleType())\
 .add("V20", DoubleType())\
 .add("V21", DoubleType())\
 .add("V22", DoubleType())\
 .add("V23", DoubleType())\
 .add("V24", DoubleType())\
 .add("V25", DoubleType())\
 .add("V26", DoubleType())\
 .add("V27", DoubleType())\
 .add("V28", DoubleType())\
 .add("Amount", DoubleType())\
 .add("Class", IntegerType())\

df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")\
    .select(from_json(col("value"), mySchema).alias("data"), "timestamp")\
    .select("data.*", "timestamp")

df1.printSchema()

# df1.writeStream\
#     .trigger(processingTime="5 seconds")\
#     .outputMode("update")\
#     .format("console")\
#     .start()\
#     .awaitTermination()

list_persons = map(lambda row: row.asDict(), df1.collect())

print(list_persons)
