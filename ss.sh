#!/usr/bin/env bash


# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/hadoop/lib/native/libhdfs.so

#set batch size if mydb data-play option is selected
MYDB_BATCH_SIZE="400" #number of messages

# path of cc configuration path
CC_CONFIG_FILEPATH="/cerebralcortex/code/ali/cc_config/cc_configuration.yml"
# data directory where all gz and json files are stored
DATA_DIR="/cerebralcortex/apiserver/data/"

# how often CC-kafka shall check for new messages (in seconds)
BATCH_DURATION="60"

# Run data replay for all participants or selected participants list define in main.py (accepted params are "all" or "selected")
PARTICIPANTS="all"

# spark master
SPARK_MASTER="spark://dagobah10dot.memphis.edu:7077"
SPARK_UI_PORT=4087

PY_FILES="/cerebralcortex/code/ali/CerebralCortex/dist/MD2K_Cerebral_Cortex-2.2.2-py3.6.egg,dist/MD2K_Cerebral_Cortex_Kafka_File_Queue_Processor-2.2.0-py3.6.egg"

python3.6 setup.py bdist_egg

spark-submit --master $SPARK_MASTER --conf spark.ui.port=$SPARK_UI_PORT --total-executor-cores 40 --conf spark.streaming.kafka.maxRatePerPartition=10 --driver-memory 1g --executor-memory 1g --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 --py-files $PY_FILES main.py -c $CC_CONFIG_FILEPATH -d $DATA_DIR -bd $BATCH_DURATION -mbs $MYDB_BATCH_SIZE -participants $PARTICIPANTS
