from __future__ import print_function

from core import CC
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, KafkaDStream

# Kafka Consumer Configs
batch_duration = 2  # seconds
ssc = StreamingContext(CC.sc, batch_duration)
broker = "localhost:9092"  # multiple brokers can be passed as comma separated values
offset_reset = "smallest"  # smallest OR largest
consumer_group_id = "md2k-test"


def spark_kafka_file_consumer(kafka_topic: str) -> KafkaDStream:
    """

    :param kafka_topic:
    :return:
    """
    return KafkaUtils.createDirectStream(ssc, kafka_topic,
                                         {"metadata.broker.list": broker, "auto.offset.reset": offset_reset,
                                          "group.id": consumer_group_id})


def spark_kafka_json_consumer(kafka_topic: str) -> KafkaDStream:
    """

    :param kafka_topic:
    """
    pass
