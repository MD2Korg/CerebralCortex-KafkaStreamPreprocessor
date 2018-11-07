# Copyright (c) 2018, MD2K Center of Excellence
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

from core.file_processor.process_csv_file import CSVToDB
from core.file_processor.process_flatbuffer import FlatbufferToDB
from cerebralcortex.core.log_manager.log_handler import LogTypes

class ProcessData(CSVToDB, FlatbufferToDB):
    def __init__(self, CC, ingestion_config, ingestion_type):
        """

        :param CC: CerebralCortex object reference
        """
        self.config = CC.config

        #self.rawData = CC.RawData
        self.nosql = CC.RawData.nosql
        #self.nosql_store = CC.RawData.nosql_store

        self.sql_data = CC.SqlData

        self.ingestion_type = ingestion_type

        self.logging = CC.logging
        self.logtypes = LogTypes()

        if self.config['visualization_storage']!="none":
            self.influxdbIP = self.config['influxdb']['host']
            self.influxdbPort = self.config['influxdb']['port']
            self.influxdbDatabase = self.config['influxdb']['database']
            self.influxdbUser = self.config['influxdb']['db_user']
            self.influxdbPassword = self.config['influxdb']['db_pass']
            self.influx_blacklist = ingestion_config["influxdb_blacklist"]

            self.influx_batch_size = 10000
            self.influx_day_datapoints_limit = 10000


        # pseudo factory
        if self.file_type == "csv":
            self.ingest = CSVToDB()
        elif self.file_type=="flatbuffer":
            self.ingest = FlatbufferToDB()
        else:
            raise ValueError(self.file_type + " is not supported to process files.")


