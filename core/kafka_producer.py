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
import os
from core import CC
from core.kafka_offset import storeOffsetRanges
from util.util import get_chunk_size
from pyspark.streaming.kafka import KafkaDStream
from util.util import row_to_datapoint, chunks, get_gzip_file_contents, rename_file
from cerebralcortex.kernel.utils.logging import cc_log


def verify_fields(msg, data_path):
    if "metadata" in msg and "filename" in msg:
        if os.path.isfile(data_path + msg["filename"]):
            return True
    return False


def file_processor(msg, data_path):
    metadata_header = msg["metadata"]

    try:
        gzip_file_content = get_gzip_file_contents(data_path + msg["filename"])
        lines = list(map(lambda x: row_to_datapoint(x), gzip_file_content.splitlines()))
        rename_file(data_path + msg["filename"])
        return [msg["filename"], metadata_header, lines]
    except Exception as e:
        error_log = "In Kafka preprocessor - Error in processing file: "+str(msg["filename"])+" - "+str(e)
        cc_log(error_log, "ERROR")
        return [msg["filename"], metadata_header, []]


def message_generator(data):
    filename = data[0]
    metadata_header = data[1]
    lines = data[2]
    result = []

    for d in chunks(lines, get_chunk_size(lines)):
        json_object = {'filename': filename, 'metadata': metadata_header, 'data': d}
        result.append(json_object)
    return result


def CC_send(data):
    for msg in data:
        print("Sending", msg['filename'], len(msg['data']))
        CC.kafka_produce_message("processed_stream", msg)


def kafka_file_to_json_producer(message: KafkaDStream, data_path):
    """
    Read convert gzip file data into json object and publish it on Kafka
    :param message:
    """

    records = message.map(lambda r: json.loads(r[1]))
    valid_records = records.filter(lambda rdd: verify_fields(rdd,data_path)).repartition(4)
    results = valid_records.map(lambda rdd: file_processor(rdd, data_path)).map(message_generator).map(CC_send)


    storeOffsetRanges(message)

    print("File Iteration count:", results.count())
