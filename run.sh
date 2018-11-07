#!/usr/bin/env bash

#########################################################################################
############################ Environment Configs ########################################
#########################################################################################

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

# export CerebralCortex path if CerebralCortex is not installed
export PYTHONPATH="${PYTHONPATH}:/home/ali/IdeaProjects/CerebralCortex-2.0/"

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/hadoop/lib/native/libhdfs.so
#sudo ln -s /usr/local/hadoop/lib/native/libhdfs.so /usr/lib/libhdfs.so

#Spark path
export SPARK_HOME=/home/ali/spark/spark-2.2.1-bin-hadoop2.7/

#PySpark args (do not change unless you know what you are doing)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"

#set spark home
export PATH=$SPARK_HOME/bin:$PATH


#########################################################################################
############################ Ingestion Configs ##########################################
#########################################################################################

# Data will be ingested in NoSQL storage if set to true. Acceptable parameters are true or false.
NOSQL_INGEST = "false"

# Data will be ingested in InfluxDB if set to true. Acceptable parameters are true or false.
INFLUXDB_INGEST = "true"

# Data ingestion source type. Whether data information (file names) will be fetched from MySQL or kafka messaging queue. Acceptable parameters are mysql or kafka only.
INGESTION_TYPE = "mysql"

# Set batch size if mysql INGESTION_TYPE is selected
# Number of MySQL row a batch shall process. One MySQL row contains all .gz files paths of one day worth of data of a stream.
MYDB_BATCH_SIZE="300"

# Set only when kafka INGESTION_TYPE is selected.
# how often CC-kafka shall check for new messages (in seconds)
BATCH_DURATION="5"

#########################################################################################
############################ YAML Config Paths and other configs ########################
#########################################################################################

# Provide a comma separated participants UUIDs. All participants' data will be processed if no UUIDs is provided.
PARTICIPANTS=""

# path of cc configuration path
CC_CONFIG_FILEPATH="/home/ali/IdeaProjects/CerebralCortex-2.0/conf/cerebralcortex.yml"

# path of ksp (kafka-stream-preprocessor) configuration path
KSP_CONFIG_FILEPATH="/home/ali/IdeaProjects/CerebralCortex-2.0/conf/data_ingestion.yml"

# spark master. This will work on local machine only. In case of cloud, provide spark master node URL:port.
SPARK_MASTER="local[*]"


spark-submit --conf spark.streaming.kafka.maxRatePerPartition=10 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 main.py -c $CC_CONFIG_FILEPATH -ksp $KSP_CONFIG_FILEPATH -ii INFLUXDB_INGEST -ni NOSQL_INGEST -bd $BATCH_DURATION -mbs $MYDB_BATCH_SIZE -participants $PARTICIPANTS