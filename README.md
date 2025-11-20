# Streaming Data Systems Assignment 4
 
## General steps before running the Flink/Spark processors:

### To start the Kafka Clusters
Navigate to the directory where the local installation of kafka is present

Run:

```bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties```

Now, to create the topics:

```bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic results  --replication-factor 1 && bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ad-events-2  --partitions 4   --replication-factor 1```


### To run the Spark processor
Navigate to the directory where the local installation of Spark is present

Run:
```./bin/spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 final_query_spark.py```

Followed by:
```python generate.py (or whatever variant of generator is to be used)```


### To run the Flink processor

Run:
```python flink_sql_query.py```

Followed by:
```python generate.py (or whatever variant of generator is to be used)```
