#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

# add current project root directory to pythonpath
#export PYTHONPATH="${PYTHONPATH}:/cerebralcortex/code/CerebralCortex/"

#Spark path
export SPARK_HOME=/usr/local/spark/

#PySpark args (do not change unless you know what you are doing)
#export PYSPARK_SUBMIT_ARGS="--packages
org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"

#set spark home
export PATH=$SPARK_HOME/bin:$PATH

# path of cc configuration path
CC_CONFIG_FILEPATH="$HOME/CerebralCortex-DockerCompose/cc_config_file/cc_configuration.yml"

# data directory where all gz and json files are stored
DATA_DIR="$HOME/CerebralCortex-DockerCompose/apiserver/data/"

# how often CC-kafka shall check for new messages (in seconds)
BATCH_DURATION="10"

# kafka broker ip with port, more than one brokers shale be separated by command
KAFKA_BROKER="127.0.0.1:9092"

# spark master
SPARK_MASTER="local[*]"

PY_FILES="$HOME/CerebralCortex/dist/MD2K_Cerebral_Cortex-2.0.0-py3.6.egg,dist/MD2K_Cerebral_Cortex_Kafka_File_Queue_Processor-2.1.0-py3.6.egg"

spark-submit --master $SPARK_MASTER --packages \
org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 --py-files $PY_FILES \
main.py -c $CC_CONFIG_FILEPATH -d $DATA_DIR -b$KAFKA_BROKER -bd $BATCH_DURATION


