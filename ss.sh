#!/usr/bin/env bash


# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/hadoop/lib/native/libhdfs.so
# path of cc configuration path
CC_CONFIG_FILEPATH="/cerebralcortex/code/ali/cc_config/cc_configuration.yml"
# data directory where all gz and json files are stored
DATA_DIR="/cerebralcortex/apiserver/data/"
# how often CC-kafka shall check for new messages (in seconds)
BATCH_DURATION="2"
# kafka broker ip with port, more than one brokers shale be separated by command
KAFKA_BROKER="dagobah10dot.memphis.edu:9092"
# spark master
SPARK_MASTER="spark://dagobah10dot.memphis.edu:7077"

PY_FILES="/cerebralcortex/code/ali/CerebralCortex/dist/MD2K_Cerebral_Cortex-2.0.0-py3.6.egg,dist/MD2K_Cerebral_Cortex_Kafka_File_Queue_Processor-2.1.0-py3.6.egg"

spark-submit --master $SPARK_MASTER --total-executor-cores 128 --executor-memory 32g --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0  --py-files $PY_FILES main.py -c $CC_CONFIG_FILEPATH -d $DATA_DIR -b $KAFKA_BROKER -bd $BATCH_DURATION -drt $DATA_REPLAY_TYPE

