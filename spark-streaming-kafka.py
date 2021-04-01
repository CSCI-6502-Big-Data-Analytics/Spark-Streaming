from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="KafkaStream")
ssc = StreamingContext(sc,10)

message = KafkaUtils.createDirectStream(ssc,topics=["creditcard"],kafkaParams={"metadata.broker.list":"localhost:9092"})
message.pprint()

ssc.start()
ssc.awaitTermination()