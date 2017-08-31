from __future__ import print_function
from core.kafka_consumer import spark_kafka_file_consumer, ssc
from core.kafka_producer import kafka_producer




def process_data(kafka_topic):

    kvs = spark_kafka_file_consumer(kafka_topic)

    kvs.foreachRDD(kafka_producer)

    ssc.start()
    ssc.awaitTermination()

process_data(["filequeue"])