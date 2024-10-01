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
@ModuleName: SInk Default Class
"""
from sink.Base import Sink
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time
import json
from pyspark.sql.functions import lit, monotonically_increasing_id


class CassandraSink(Sink):
    """
        Writes the queried data in to cassandra table.
        It creates the table with given trainingjob name
        It creates Keyspaces if doesn't exist.
    @Class: Default Spark Sink
    """

    def __init__(self, classflavour):
        """
        @Methond: Constructor
        @Input : classflavor
        """
        self.class_type = "Custom"
        self.flavour = classflavour
        self.logger = None
        self.sparkloadkey = None
        self.writetype = None
        self.tableName = None
        self.keyspace = None
        self.user = None
        self.passw = None
        self.host = None
        self.port = None

    def init(self, sparkhelper, confighelper, inputdict):
        """
        @Methond:init
        @Inputs: sparkhelper
                 confighelper
                 inputdict
        """
        self.logger = confighelper.getLogger()

        self.tableName = inputdict["CollectionName"]
        classconfig, sparkconfig = confighelper.getClassConfig(self)
        env_conf = confighelper.getEnvConfig()

        self.keyspace = env_conf["cassandra_sinkdb"]
        self.host = env_conf["fs_db_ip"]
        self.port = env_conf["fs_db_port"]
        self.user = env_conf["fs_db_user"]
        self.passw = env_conf["fs_db_password"]

        self.sparkloadkey = classconfig["SparkLoadKey"]
        self.writetype = classconfig["WriteMode"]

        self.logger.debug(
            "Sink keyspace-" + str(self.keyspace) + " table-" + str(self.tableName)
        )

        for key in sparkconfig:
            value = sparkconfig[key]
            print("Spark Config", key, sparkconfig[key])
            if value.startswith("$Input$"):
                inputkey = value[7:]
                sparkhelper.add_conf(key, inputdict[inputkey])
            else:
                sparkhelper.add_conf(key, value)

    def write(self, sparksession, sparkdf):
        """
        @Method: write the data from sparkdf in to cassandra table
        @input : sparksession
                 sparkdf
        """
        self.logger.debug("Data writing to Sink " + self.flavour)
        self.create_table(sparkdf)
        sparkdf = sparkdf.select("*") \
                .withColumn("_partition_key", lit('1')) \
                .withColumn("_Id", monotonically_increasing_id())



        write_options = {
            "table": self.tableName,
            "keyspace": self.keyspace,
            "spark.cassandra.connection.host": self.host,
            "spark.cassandra.connection.port": self.port,
            "spark.cassandra.auth.username": self.user,
            "spark.cassandra.auth.password": self.passw,
        }

        sparkdf.write.format(self.sparkloadkey).options(**write_options).mode(
            "append"
        ).save()

        self.logger.debug(
            "*** Data written to Sink *** " + self.keyspace + "." + self.tableName
        )



    def create_table(self, sparkdf):
        """
        Recreates table in cassandra db as per trainingjob name
        Creates Keyspaces , if doesn't exist
        """
        self.logger.debug("Creating table...")

        auth_provider = PlainTextAuthProvider(username=self.user, password=self.passw)
        cluster = Cluster([self.host], port=self.port, auth_provider=auth_provider)

        session_default = cluster.connect()
        create_ks_query = self.buildKeyspaceQuery(self.keyspace)
        session_default.execute(create_ks_query)

        session = cluster.connect(self.keyspace)


        session.execute(self.buildDeleteTable())

        query = self.buildCreateTable(sparkdf)
        session.execute(query)
        time.sleep(3)
        cluster.shutdown()

    def buildCreateTable(self, sparkdf):
        """
        Builds simple cassandra query for creating table.
        Columns names as per sparkdf headers,
        choosing _partition_key in sparkdf as partition key,
        _Id as clustering key for table
        """
        query = "CREATE TABLE " + self.tableName + ' ( "_partition_key" text, "_Id" bigint, '
        if sparkdf is not None:
            col_list = sparkdf.schema.names
            # To maintain the column name case sensitivity
            for col in col_list:
                query = query + ' "' + str(col) + '" text ' + ","

        # Creating partition_key column as Partition Key, _Id as Clustering Key
        query = query + 'PRIMARY KEY (("_partition_key"), "_Id"));'
        self.logger.debug("Create table query " + query)
        return query

    def buildDeleteTable(self):
        """
        Builds simple cassandra query for deleting table
        """
        query = "DROP TABLE IF EXISTS  " + self.tableName + " ;"
        self.logger.debug("Delete table query " + query)
        return query

    def buildKeyspaceQuery(self, keyspace):
        """
        Builds cassandra query for creating keyspace 1
        """
        query = (
            "CREATE KEYSPACE IF NOT EXISTS "
            + keyspace
            + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor'  : 2 };"
        )
        return query
