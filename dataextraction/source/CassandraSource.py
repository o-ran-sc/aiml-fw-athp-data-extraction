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
@Module: Cassandra Spark Source
"""
from source.Base import Source
import json
from pyspark.sql import SQLContext, SparkSession

class CassandraSource(Source):
    """
    @Class: Spark Cassandra Source
    """
    def __init__(self,classflavour):
        """
        @Method: Single Arg Constructor
        """
        self.class_type="Custom"
        self.flavour=classflavour
        self.logger=None
        self.sparkloadkey=None
        
    def init(self,sparkhelper,confighelper,inputdict):
        """
        @Method:init
        @Inputs: sparkhelper, confighelper , inputdict
        @Output: None
        """
        self.logger = confighelper.getLogger()
        self.logger.debug("Set Cassandra Class Spark Source Flavor: " + self.flavour)
        classconfig,sparkconfig  = confighelper.getClassConfig(self)
        env_conf = confighelper.getEnvConfig()
        self.sparkloadkey = classconfig["SparkLoadKey"]
        self.keyspace  = env_conf['cassandra_sourcedb']
        self.table = env_conf['cassandra_sourcetable']
        
        self.logger.debug("Source keyspace:" +  str(self.keyspace) + " table-" + str(self.table))
        

        for key in sparkconfig:
            value = sparkconfig[key]
            if value.startswith('$Input$'):
                inputkey = value[7:]
                sparkhelper.add_conf(key,inputdict[inputkey])
            else:
                sparkhelper.add_conf(key,value)
            
        self.logger.debug("Spark cassandra source initialized as" + self.flavour)

    def load(self,sparksession):
        """
        @Method: load 
        @Inputs: sparksession
        @Output: dataframe
            Fetches data from cassandra database
        """
        
        df_data = sparksession.read.format(self.sparkloadkey).options(table=self.table,keyspace=self.keyspace).load()
        self.logger.debug("Data Loaded from cassandra Source: " + self.keyspace + "." + self.table)
        return df_data
