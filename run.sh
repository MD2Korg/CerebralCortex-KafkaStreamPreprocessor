#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

# export CerebralCortex path if CerebralCortex is not installed
export PYTHONPATH="${PYTHONPATH}:/home/ali/IdeaProjects/CerebralCortex-2.0/"

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/hadoop/lib/native/libhdfs.so
#sudo ln -s /opt/hadoop/lib/native/libhdfs.so /usr/lib/libhdfs.so

#Spark path
export SPARK_HOME=/home/ali/spark/spark-2.2.1-bin-hadoop2.7/

#PySpark args (do not change unless you know what you are doing)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 pyspark-shell"

#set spark home
export PATH=$SPARK_HOME/bin:$PATH

#use mydb to process messages without publishing them on kafka
DATA_REPLAY_TYPE="mydb" #acceptable params are mydb or kfka
MYDB_BATCH_SIZE="5000" #number of messages

# path of cc configuration path
CC_CONFIG_FILEPATH="/home/ali/IdeaProjects/CerebralCortex-2.0/cerebralcortex/core/resources/cc_configuration.yml"
# data directory where all gz and json files are stored
DATA_DIR="/home/ali/IdeaProjects/MD2K_DATA/data/"
# how often CC-kafka shall check for new messages (in seconds)
BATCH_DURATION="5"
# kafka broker ip with port, more than one brokers shale be separated by command
KAFKA_BROKER="127.0.0.1:9092"

# spark master
SPARK_MASTER="local[*]"

spark-submit --conf spark.streaming.kafka.maxRatePerPartition=10 --driver-memory 1g --executor-memory 1g --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 main.py -c $CC_CONFIG_FILEPATH -d $DATA_DIR -b $KAFKA_BROKER -bd $BATCH_DURATION -drt $DATA_REPLAY_TYPE -mbs $MYDB_BATCH_SIZE