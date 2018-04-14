# Copyright (c) 2017, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
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


from cerebralcortex.core.util.spark_helper import get_or_create_sc
from pyspark.streaming import StreamingContext
from cerebralcortex.cerebralcortex import CerebralCortex
from core.process_messages import kafka_file_to_json_producer, mysql_batch_to_db
from core.kafka_consumer import spark_kafka_consumer
import argparse
import json


def run():
    selected_participants = [
        "622bf725-2471-4392-8f82-fcc9115a3745",
        "d3d33d63-101d-44fd-b6b9-4616a803225d",
        "c1f31960-dee7-45ea-ac13-a4fea1c9235c",
        "7b8358f3-c96a-4a17-87ab-9414866e18db",
        "8a3533aa-d6d4-450c-8232-79e4851b6e11",
        "e118d556-2088-4cc2-b49a-82aad5974167",
        "260f551d-e3c1-475e-b242-f17aad20ba2c",
        "dd13f25f-77a0-4a2c-83af-bb187b79a389",
        "17b07883-4959-4037-9b80-dde9a06b80ae",
        "5af23884-b630-496c-b04e-b9db94250307",
        "61519ad0-2aea-4250-9a82-4dcdb93a569c",
        "326a6c55-c963-42c2-bb8a-2591993aaaa2",
        "a54d9ef4-a46a-418b-b6cc-f10b49a946ac",
        "2fb5e890-afaf-428a-8e28-a7c70bf8bdf1",
        "c93a811e-1f47-43b6-aef9-c09338e43947",
        "9e4aeae9-8729-4b0f-9e84-5c1f4eeacc74",
        "479eea59-8ad8-46aa-9456-29ab1b8f2cb2",
        "b4ff7130-3055-4ed1-a878-8dfaca7191ac",
        "fbd7bc95-9f42-4c2c-94f4-27fd78a7273c",
        "bbc41a1e-4bbe-4417-a40c-64635cc552e6",
        "82a921b9-361a-4fd5-8db7-98961fdbf25a",
        "66a5cdf8-3b0d-4d85-bdcc-68ae69205206",
        "d4691f19-57be-44c4-afc2-5b5f82ec27b5",
        "136f8891-af6f-49c1-a69a-b4acd7116a3c"
    ]
    parser = argparse.ArgumentParser(description='CerebralCortex Kafka Message Handler.')
    parser.add_argument("-c", "--config_filepath", help="Configuration file path", required=True)
    parser.add_argument("-d", "--data_dir", help="Directory path where all the gz files are stored by API-Server",
                        required=True)

    parser.add_argument("-bd", "--batch_duration",
                        help="How frequent kafka messages shall be checked (duration in seconds)", default="5", required=False)


    parser.add_argument("-mbs", "--mydb_batch_size",
                        help="Total number of messages to fetch from MySQL for processing.", default="5000",
                        required=True)

    parser.add_argument("-participants", "--participants",
                        help="Whether run data replay on all participants or select one.", default="all",
                        required=False)

    args = vars(parser.parse_args())



    participants = args["participants"]
    mydb_batch_size = args["mydb_batch_size"]
    config_filepath = str(args["config_filepath"]).strip()
    batch_duration = int(args["batch_duration"])
    data_path = str(args["data_dir"]).strip()
    if (data_path[-1] != '/'):
        data_path += '/'


    # Kafka Consumer Configs
    spark_context = get_or_create_sc(type="sparkContext")
    spark_context.setLogLevel("WARN")
    consumer_group_id = "md2k-test"

    CC = CerebralCortex(config_filepath)
    broker = str(CC.config["kafkaserver"]["host"])+":"+str(CC.config["kafkaserver"]["port"])
    data_replay_using = str(CC.config["data_replay"]["replay_type"])

    if data_replay_using=="mydb":
        for replay_batch in CC.SqlData.get_replay_batch(record_limit=mydb_batch_size):
            new_replay_batch = []
            #get records from mysql and process (skip kafka)
            if participants=="all":
                new_replay_batch = replay_batch
            else:

                for rb in replay_batch:
                    if rb["owner_id"] in selected_participants:
                        new_replay_batch.append(rb)
            mysql_batch_to_db(spark_context, new_replay_batch, data_path, config_filepath)

    else:
        ssc = StreamingContext(spark_context, batch_duration)
        kafka_files_stream = spark_kafka_consumer(["filequeue"], ssc, broker, consumer_group_id, CC)
        if kafka_files_stream is not None:
            kafka_files_stream.foreachRDD(lambda rdd: kafka_file_to_json_producer(rdd, data_path, config_filepath, CC))

        ssc.start()
        ssc.awaitTermination()


if __name__ == "__main__":
    run()
