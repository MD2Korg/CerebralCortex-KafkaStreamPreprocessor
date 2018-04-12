#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6
export LD_LIBRARY_PATH=/home/vagrant/hadoop/lib/native/
export PATH=/home/vagrant/hadoop/bin/:$PATH

#Spark path
export SPARK_HOME=/usr/local/spark/

#set batch size if mydb data-play option is selected
MYDB_BATCH_SIZE="500" #number of messages

#set spark home
export PATH=$SPARK_HOME/bin:$PATH

# path of cc configuration path
CC_CONFIG_FILEPATH="/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml"

# data directory where all gz and json files are stored
DATA_DIR="/home/vagrant/CerebralCortex-DockerCompose/data/"

# how often CC-kafka shall check for new messages (in seconds)
BATCH_DURATION="10"

# spark master
SPARK_MASTER="local[2]"


spark-submit --master $SPARK_MASTER --packages \
org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
main.py -c $CC_CONFIG_FILEPATH -d $DATA_DIR -bd $BATCH_DURATION -mbs $MYDB_BATCH_SIZE

