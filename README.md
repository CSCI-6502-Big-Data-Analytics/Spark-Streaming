# Data

1. Download csv from [Kaggle](https://www.kaggle.com/mlg-ulb/creditcardfraud).
2. Save the csv in a directory called `data`.

# Kafka stream
1. Run `docker-compose up`.
2. Open a terminal window and run `python producer.py`.
3. Open another terminal window and run `python consumer.py`.

# Spark-Streaming

1. Run `docker-compose up`.
2. Install the packages mentioned in requirements.txt
3. If you're using Windows: Download the Hadoop bin files from [here](https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip). Next follow [this StackOverflow Answer](https://stackoverflow.com/a/50639840) and add `HADOOP_HOME` to environment variables. Spark should be good to go now.
4. Run `python producer.py` to start the [Kafka producer](./producer.py).
5. Run the command `spark-submit --jars spark-streaming-kafka-0-8-assembly.jar spark-streaming-kafka.py`
