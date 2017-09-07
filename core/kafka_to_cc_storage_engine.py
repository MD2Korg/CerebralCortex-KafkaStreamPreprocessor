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
from cerebralcortex.kernel.datatypes.datastream import DataStream, DataPoint
from dateutil.parser import parse

from pyspark.streaming.kafka import KafkaDStream


def verify_fields(msg):
    if "metadata" in msg and "data" in msg:
        return True
    return False

def kafka_to_db(message: KafkaDStream):
    """

    :param message:
    """

    records = message.map(lambda r: json.loads(r[1]))
    valid_records = records.filter(verify_fields).repartition(8)

    results = valid_records.map(json_to_datastream)
    CC.save_datastream(results)


def json_to_datastream(msg) -> DataStream:
    """
    :param metadata_header:
    :param json_data:
    :return:
    """
    metadata_header = msg["metadata"]
    json_data = msg["data"]

    data = json_to_datapoint(json_data)
    if not "execution_context" in metadata_header:
        raise ValueError("Execution context cannot be empty.")
    elif not "identifier" in metadata_header:
        raise ValueError("Stream ID cannot be empty.")
    elif not "owner" in metadata_header:
        raise ValueError("Stream owner ID cannot be empty.")
    elif not "name" in metadata_header:
        raise ValueError("Stream name cannot be empty.")

    # Metadata fields
    streamID = metadata_header["identifier"]
    ownerID = metadata_header["owner"]
    name = metadata_header["name"]
    data_descriptor = {"data_descriptor": metadata_header["data_descriptor"] if "data_descriptor" in metadata_header else ""}
    execution_context = {"execution_context": metadata_header["execution_context"]}
    annotations = {"annotations": metadata_header["annotations"] if "annotations" in metadata_header else ""}
    stream_type = "stream"  # TODO: stream-type is missing in metadata
    start_time = parse(parse(json_data[0]["starttime"]).strftime("%Y-%m-%d %H:%M:%S"))
    end_time = parse(parse(json_data[len(json_data) - 1]["starttime"]).strftime("%Y-%m-%d %H:%M:%S"))

    return DataStream(streamID, ownerID, name, data_descriptor, execution_context, annotations,
                      stream_type, start_time, end_time, data)


def json_to_datapoint(data: List) -> List:
    """

    :param data:
    :return:
    """
    datapointsList = []
    for row in data:
        dp = DataPoint(parse(row["starttime"]), "", row["value"])
        datapointsList.append(dp)
    return datapointsList
