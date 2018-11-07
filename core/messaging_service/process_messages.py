# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import json
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.data_manager.raw.file_to_db import FileToDB
from core.file_processor.data import ProcessData
from pyspark.streaming.kafka import KafkaDStream


def save_data(msg, data_path, config_filepath, ingestion_config, ingestion_type, nosql_in, influxdb_in):
    CC = CerebralCortex(config_filepath)
    process_data = ProcessData(CC, ingestion_config, ingestion_type)
    process_data.ingest.file_processor(msg, data_path, influxdb_in, nosql_in)

#########################################################################################
######################### MySQL Based Data Ingestion ####################################
#########################################################################################

def mysql_batch_to_db(spark_context, replay_batch, data_path, config_filepath, ingestion_config, ingestion_type, nosql_in, influxdb_in):
    if len(replay_batch)>0:
        message = spark_context.parallelize(replay_batch)
        message.foreach(lambda msg: save_data(msg, data_path, config_filepath, ingestion_config, ingestion_type, nosql_in, influxdb_in))
        print("File Iteration count:", len(replay_batch))

#########################################################################################
######################### KAFKA Based Data Ingestion ####################################
#########################################################################################

def verify_fields(msg: dict) -> bool:
    """
    Verify whether msg contains file name and metadata
    :param msg:
    :param data_path:
    :return:
    """
    if "metadata" in msg and "filename" in msg and "day" in msg:
        return True
    return True

def kafka_msg_to_db(message: KafkaDStream, data_path, config_filepath, CC):
    """
    Read convert gzip file data into json object and publish it on Kafka
    :param message:
    """

    records = message.map(lambda r: json.loads(r[1]))
    valid_records = records.filter(lambda rdd: verify_fields(rdd))
    results = valid_records.map(lambda msg: save_data(msg, data_path, config_filepath))
    print("File Iteration count:", results.count())
    store_offset_ranges(message, CC)

def store_offset_ranges(rdd, CC):
    offsetRanges = rdd.offsetRanges()
    for offsets in offsetRanges:
        try:
            CC.store_or_update_Kafka_offset(offsets.topic, offsets.partition, offsets.fromOffset, offsets.untilOffset)
        except Exception as e:
            print(e)