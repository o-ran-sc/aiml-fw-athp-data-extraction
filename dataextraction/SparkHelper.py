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
@Module:Spark Session Manager
"""
import configparser
from pyspark import SparkConf
from pyspark.sql import SparkSession
from ConfigHelper import ConfigHelper

class sparkSessionManager():
    """
    @Module:Spark Session Manager
    """
    def __init__(self):
        """
        @Constructor:Spark Session Manager
        """
        config = configparser.ConfigParser()
        confighelp = ConfigHelper()
        self.logger = confighelp.getLogger()
        self.loglevel = confighelp.log_level
        code_path = confighelp.exec_path
        if code_path is None:
            config.read("SparkConfig.ini")
        else:
            config.read(code_path + "/SparkConfig.ini")
        base = config['BaseConfig']
        self.appname = base["DefaultAppName"]
        self.master = base["DefaultMaster"]
        if base["Override_Log_Level"] is not None:
            self.loglevel = base["Override_Log_Level"]
        self.sconf = SparkConf()
        self.addtionalconfig = config["ExtraConfig"]
    def addConf(self, key, value):
        """
        @Function: Adding configuration as runtime
        """
        self.sconf.set(key, value)
        
    def getAllConf(self):
        self.logger.debug("*********** ALL CONF *** " + str(self.sconf.getAll()))
    
    def getSession(self):
        """
        @Function: get Spark Session
        """
        for key in self.addtionalconfig:
            self.sconf.set(key, self.addtionalconfig[key])
        try:
            if hasattr(self, 'spark'):
            # pylint: disable=E0203
                self.spark.stop()
        # pylint: disable=W0201
            self.spark = SparkSession.builder.\
            appName(self.appname).\
            master(self.master).\
            config(conf=self.sconf).getOrCreate()
            self.spark.sparkContext.setLogLevel(self.loglevel)
            return self.spark
        except Exception as exp:
            raise  Exception('Error Building Spark Session').with_traceback(exp.__traceback__)
    def stop(self):
        """
        @Function:Stop Spark Session
        """
        if hasattr(self, 'spark') and self.spark is not None:
            self.spark.stop()
