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

import sys
import os
from dotenv import load_dotenv
from mock import patch

# In order to import dataextraction functions
sys.path.extend(["dataextraction/"])
os.environ['CODE_DIR_PATH']='test'   
load_dotenv('test/test_env.env') # Loading Env Variables 
from sink.CassandraSink import CassandraSink
from ConfigHelper import ConfigHelper

class sparkSessionHelper():
    def __init__(self):
        pass

    def addConf(self, key, value):
        pass

class df_helper():
    def __init__(self):
        self.schema = self
        self.names = ['Id', 'DLPRB']
        self.write = self
        self.saved = False

    def select(self, query):
        return self
    
    def withColumn(self, col_name, order):
        return self
    
    def format(self, key):
        return self
    
    def options(self, **kwargs):
        return self
    
    def mode(self, mode):
        return self
    
    def save(self):
        self.saved = True


class Test_CassandraSink():
    def setup_method(self):
        self.obj = CassandraSink("InfluxDb")
        fsConf = ConfigHelper()
        input_dict = {'CollectionName': 'last_check3'}
        self.obj.init(sparkSessionHelper(), fsConf, input_dict)

    def test_init(self):
        assert self.obj.logger != None, "Cassandra Sink Object Creation Failed"
    
    @patch('sink.CassandraSink.Cluster')
    @patch('sink.CassandraSink.Cluster.connect')
    @patch('sink.CassandraSink.monotonically_increasing_id')
    def test_write(self, mock1, mock2, mock3):
        df_helper_obj = df_helper()
        self.obj.write(sparkSessionHelper(),df_helper_obj)
        assert df_helper_obj.saved, "Data Failed to save"