from __future__ import print_function

import os
import sys
import shutil
import json

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils

OUTPUT_PATH = '/tmp/spark/checkpoint_01'


def get_sql_query():
    """
    Get the SQL Template
    :return: str
    """
    try:
        with open('sql/transaction_calc.sql', 'r') as f:
            return f.read()
    except Exception as e:
        print('--> Opps! Can\'t open the sql file')


def get_spark_session_instance(spark_conf):
    """
    Lazily instantiated global instance of SparkSession
    :param spark_conf: spark configuration
    :return: SparkSession
    """
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def process(time, rdd):
    """
    What to do per each RDD
    :param time:
    :param rdd:
    :return:
    """
    print('==============----> %s <----===============' % str(time))

    try:
        spark = get_spark_session_instance(rdd.context.getConf())

        row_rdd = rdd.map(lambda w: Row(city=w['city'], currency=w['currency'], amount=w['amount']))

        test_data_frame = spark.createDataFrame(row_rdd)
        test_data_frame.createOrReplaceTempView('treasury_stream')

        sql_query = get_sql_query()
        test_result_data_frame = spark.sql(sql_query)
        test_result_data_frame.show(5)

        # insert into DB
        try:
            test_result_data_frame.write\
                .format('jdbc')\
                .mode('append')\
                .option('driver', 'org.postgresql.Driver')\
                .option('url', 'amazon...')\
                .option('dbtable', 'transactio_flow')\
                .option('user', 'foo')\
                .option('password', 'bar')\
                .save()
        except Exception as e:
            print('--> Opps! Error: {}'.format(e))

    except Exception as e:
        print('--> Opps! Error: {}'.format(e))


def create_context():
    """
    Spark Context creation
    :return: StreamingContext
    """
    sc = SparkContext(appName='PythonStreamingKafkaTransaction')
    sc.setLogLevel('ERROR')

    ssc = StreamingContext(sc, 2)

    broker_list, topic = sys.argv[1:]

    try:
        direct_kafka_stream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': broker_list})
    except Exception:
        raise ConnectionError('Kafka error: Connection refused: broker_list={} topic={}'.format(broker_list, topic))

    parsed_lines = direct_kafka_stream.map(lambda v: json.loads(v[1]))

    # RDD handling
    parsed_lines.foreachRDD(process)

    return ssc


# --------------------------------------------------------------------
# Begin
# --------------------------------------------------------------------
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: spark_job.py <zookeeper> <topic>', file=sys.stderr)
        exit(1)

    print('--> Creating new context')
    if os.path.exists(OUTPUT_PATH):
        shutil.rmtree(OUTPUT_PATH)

    ssc = StreamingContext.getOrCreate(OUTPUT_PATH, lambda: create_context())
    ssc.start()
    ssc.awaitTermination()
