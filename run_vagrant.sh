#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6
export LD_LIBRARY_PATH=/home/vagrant/hadoop/lib/native/
export PATH=/home/vagrant/hadoop/bin/:$PATH

#Spark path
export SPARK_HOME=/usr/local/spark/

#use mydb to process messages without publishing them on kafka
DATA_REPLAY_TYPE="mydb" #acceptable params are mydb or kfka

#set spark home
export PATH=$SPARK_HOME/bin:$PATH

# path of cc configuration path
CC_CONFIG_FILEPATH="/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml"

# data directory where all gz and json files are stored
DATA_DIR="/home/vagrant/CerebralCortex-DockerCompose/data/"

# how often CC-kafka shall check for new messages (in seconds)
BATCH_DURATION="10"

# kafka broker ip with port, more than one brokers shale be separated by command
KAFKA_BROKER="127.0.0.1:9092"

# spark master
SPARK_MASTER="local[2]"


spark-submit --master $SPARK_MASTER --packages \
org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
main.py -c $CC_CONFIG_FILEPATH -d $DATA_DIR -b$KAFKA_BROKER -bd $BATCH_DURATION -drt $DATA_REPLAY_TYPE

