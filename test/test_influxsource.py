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
from source.InfluxSource import InfluxSource
from ConfigHelper import ConfigHelper



class helper():
    def __init__(self):
        pass

    def createDataFrame(self, data_df):
        return "Data Loaded Successfully"

class Test_InfluxSource():
    def setup_method(self):
        # self.obj_zero_args = InfluxSource()
        self.obj_one_arg = InfluxSource("InfluxDb")
        fsConf = ConfigHelper()
        input_dict = {'query': 'Select * from last_check3'}
        self.obj_one_arg.init(None, fsConf, input_dict)

    def test_init(self):
        assert self.obj_one_arg.logger != None, "Influx Source Object Creation Failed"
    
    def test_negative_init(self):
        influx_source_obj = InfluxSource("InfluxDb")
        fsConf = ConfigHelper()
        input_dict = {'dummy-key': 'dummy-value'}
        influx_source_obj.init(None, fsConf, input_dict)
        assert influx_source_obj.logger != None, "Influx Source Object Creation Failed"

    @patch('source.InfluxSource.InfluxDBClient')
    def test_load(self, mock1):
        out = self.obj_one_arg.load(helper())
        assert out == "Data Loaded Successfully", "Data Failed to Load"
