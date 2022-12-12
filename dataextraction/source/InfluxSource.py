# ==================================================================================
#
#       Copyright (c) 2022 Samsung Electronics Co., Ltd. All Rights Reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# ==================================================================================

"""
@Module : Influx DB Source
"""
from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from source.Base import Source

class InfluxSource(Source):
    """
    @Class : Influx DB Source
    """
    def __init__(self,classflavour):
        """
        @Method: Single Arg Constructor
        """
        self.class_type="Custom"
        self.flavour=classflavour
        self.logger  = None
        self.classconfig  = None
        self.url     = None
        self.token   = None
        self.org     = None
        self.timeout = None
        self.ssl     = None
        self.query = None
    
    def init(self, sparkhelper, confighelper, inputdict):
        """
        @Method: init
        @Input : spark - Spark
                 confighelper - config helper
                 inputdict - input dictionary
        @Output: None
        """
        self.logger  = confighelper.getLogger()
        classconfig  = confighelper.getClassConfig(self)
        self.url     = classconfig["url"]
        self.token   = classconfig["token"]
        self.org     = classconfig["org"]
        self.timeout = classconfig["timeout"]
        self.ssl     = classconfig["ssl"]
        self.query   = classconfig["Query"]
        if "query" in inputdict.keys():
            self.query = inputdict["query"]
            self.logger.debug("Set variable query"+ self.query)
        else:
            self.logger.error("Expected variable query"+ self.query)
    def load(self,sparksession):
        """
        @Method: init
        @Input : spark - Spark
        @Output: spark dataframe
        """
        client = InfluxDBClient(url=self.url,\
                                token = self.token,org=self.org,\
                                timeout=self.timeout,\
                                verify_ssl=self.ssl)
        query_api = client.query_api()
        self.logger.debug("Started Data Extraction for Influx Source "+self.query)
        df_data = query_api.query_data_frame(self.query)
        spark_df = sparksession.createDataFrame(df_data)
        client.close()
        self.logger.debug("Load Data Completed for Influx Source")
        return spark_df
