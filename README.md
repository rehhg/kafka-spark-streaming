# Example of usage Spark Streaming with Apache Kafka

#### Spark streaming handles message flows from Apache Kafka and saves it to AWS RDS PostgreSQL

##### 1) First you need to create credentials.yaml file with structure:
    credentials:
        username: foo
        password: bar
        db_url: (url to your psotgresql database on the cloud)

##### 2) Then install Spark and Kafka

#### Execution:

```console
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,\
org.postgresql:postgresql:9.4.1207 \
spark_job.py localhost:9092 transaction
```