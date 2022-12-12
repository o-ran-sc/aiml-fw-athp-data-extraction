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

import pytest
import sys
import os
from dotenv import load_dotenv
sys.path.extend(["dataextraction/"])
from source.DefaultSparkSource import DefaultSparkSource
from ConfigHelper import ConfigHelper

class helper2:
    def __init__(self):
        pass
    def format(self, input1):
        return Spark_helper()

class Spark_helper:
    def __init__(self):
        self.test_result = False
        self.read = helper2()
    
    def add_conf(self, key, value):
        self.test_result = True
    
    def load(self):
        return 'Spark Session Loaded'
    


class Test_DefaultSparkSource:
    def setup_method(self):
        os.environ['CODE_DIR_PATH'] = 'test'
        load_dotenv('test/test_env.env')
        self.obj = DefaultSparkSource("CassandraSource")
        


    def test_init(self):
        assert self.obj != None, 'DefaultSparkSource (Single args) Object Creation, Failed'
        
    def test_init3(self):
        load_dotenv('test/test_env.env')
        spark_helper_obj = Spark_helper()
        self.obj.init(spark_helper_obj, ConfigHelper(), {})
        assert spark_helper_obj.test_result, 'Intialisation of Spark Source Failed'

    def test_load(self):
        spark_helper_obj = Spark_helper()
        self.obj.init(spark_helper_obj, ConfigHelper(), {})
        out = self.obj.load(spark_helper_obj)

        assert out == 'Spark Session Loaded' , 'Data Extraction Failed'