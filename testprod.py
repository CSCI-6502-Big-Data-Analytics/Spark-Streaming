
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .master('local')
         .appName('fraud-detection')
         # Add kafka package
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())
sc = spark.sparkContext

