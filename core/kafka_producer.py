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
from pyspark.streaming.kafka import KafkaDStream
from util.util import row_to_datapoint, chunks, get_gzip_file_contents, rename_file


def kafka_file_to_json_producer(message: KafkaDStream):
    """
    Read convert gzip file data into json object and publish it on Kafka
    :param message:
    """
    records = message.collect()
    for record in records:
        msg = json.loads(record[1])
        if "metadata" in msg and "filename" in msg:
            metadata_header = msg["metadata"]
            if os.path.isfile(msg["filename"]):
                gzip_file_content = get_gzip_file_contents(msg["filename"])
                lines = list(map(lambda x: row_to_datapoint(x), gzip_file_content.splitlines()))

                for d in chunks(lines, 1000):
                    json_object = {'metadata': metadata_header, 'data': d}
                    CC.kafka_produce_message("processed_stream", json_object)
                rename_file(msg["filename"])
            print("PROCESSED - " + msg["filename"])
        else:
            raise ValueError("Kafka message does not contain metadata and/or file-name.")
