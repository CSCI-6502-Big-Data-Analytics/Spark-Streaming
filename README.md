# Data

1. Download csv from [Kaggle](https://www.kaggle.com/mlg-ulb/creditcardfraud).
2. Save the csv in a directory called `data`.

# Kafka stream
1. Run `docker-compose up`.
2. Open a terminal window and run `python old_producer.py`.
3. Open another terminal window and run `python consumer.py`.

# Spark-Streaming
For streaming, we tried to simulate a real world scenario, where instead of reading data from a csv file, we'll be getting data from an api. We thus used flask to create this mock api.

1. Run `docker-compose up`.
2. Install the packages mentioned in requirements.txt
3. **If you're using Windows**: Download the Hadoop bin files from [here](https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip). Next follow [this StackOverflow Answer](https://stackoverflow.com/a/50639840) and add `HADOOP_HOME` to environment variables. Spark should be good to go now.
4. Run `python new_producer.py` to start the [Kafka producer](./new_producer.py).
5. Run the command `spark-submit --jars spark-streaming-kafka-0-8-assembly.jar spark-streaming-kafka.py`
6. Open up an API testing tool like Postman and hit the endpoint: `http://localhost:5000/predict` with the a json body in the following format:

    ```json
    {        
      "Time": 99,
      "V1": "-0.8839956497728281",
      "V2": "-0.150764822957996",
      "V3": "2.2917907214775495",
      "V4": "-0.26345226832778196",
      "V5": "-0.8145352842846351",
      "V6": "0.955840627763703",
      "V7": "0.0976306732312271",
      "V8": "0.474046969090009",
      "V9": "0.139512299928856",
      "V10": "-0.7298612019237679",
      "V11": "0.711062608544338",
      "V12": "0.095006434720644",
      "V13": "-1.09750534430121",
      "V14": "-0.0597015195949617",
      "V15": "0.23455722529490802",
      "V16": "-0.142193908419341",
      "V17": "0.193357555365588",
      "V18": "0.21785331399354502",
      "V19": "1.1555711211730901",
      "V20": "0.35875101019677796",
      "V21": "0.0709014399364962",
      "V22": "0.0518320695040774",
      "V23": "0.110297657345214",
      "V24": "-0.260628692852532",
      "V25": "-0.0975487192089246",
      "V26": "1.15543923721475",
      "V27": "-0.0211993299630798",
      "V28": "0.0625654360473211",
      "Amount": 142.71,
      "Class": 0
    }
```
