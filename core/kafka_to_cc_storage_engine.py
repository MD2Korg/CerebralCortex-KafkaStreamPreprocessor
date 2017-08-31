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
from typing import List
from core import CC
from pyspark.streaming.kafka import KafkaDStream

from cerebralcortex.kernel.datatypes.datastream import DataStream, DataPoint


def kafka_to_db(message: KafkaDStream):
    """

    :param message:
    """
    records = message.collect()
    for record in records:
        msg = json.loads(record[1])
        if "metadata" in msg and "data" in msg:
            metadata_header = msg["metadata"]
            data = msg["data"]
            datastream = json_to_datastream(metadata_header, data)
            CC.save_datastream(datastream)
        else:
            raise ValueError("Kafka message does not contain metadata and/or data.")


def json_to_datastream(metadata: dict, json_data: dict) -> DataStream:
    """
    :param metadata:
    :param json_data:
    :return:
    """
    data = json_to_datapoint(json_data)

    #Metadata fields
    streamID = metadata["identifier"]
    ownerID = metadata["owner"]
    name = metadata["name"]
    data_descriptor = metadata["data_descriptor"]
    execution_context = metadata["execution_context"]
    annotations = metadata["annotations"]
    stream_type = "stream" #TODO: stream-type is missing in metadata
    start_time = json_data[0]["starttime"]
    end_time = json_data[len(json_data)-1]["starttime"]

    return DataStream(streamID, ownerID, name, data_descriptor, execution_context, annotations,
                      stream_type, start_time, end_time, data)

def json_to_datapoint(data: List) -> List:
    """

    :param data:
    :return:
    """
    datapointsList = []
    for row in data:
        dp = DataPoint(row["starttime"], "", row["value"])
        datapointsList.append(dp)
    return dp
