from __future__ import print_function

import json

from core import CC
from util.util import row_to_datapoint, chunks, get_gzip_file_contents


def kafka_producer(message):
    records = message.collect()
    for record in records:
        msg = json.loads(record[1])
        if "metadata" in msg and "filename" in msg:
            metadata_header = msg["metadata"]
            gzip_file_content = get_gzip_file_contents(msg["filename"])
            lines = list(map(lambda x: row_to_datapoint(x), gzip_file_content.splitlines()))
            for d in chunks(lines, 1000):
                json_object = {'metadata': metadata_header, 'data': d}
                CC.kafka_produce_message("stream_processed", json.dumps(json_object))
        else:
            raise ValueError("Kafka message does not contain metadata or file-name.")
