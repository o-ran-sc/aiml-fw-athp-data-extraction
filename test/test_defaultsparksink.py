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

load_dotenv('test/test_env.env') 
os.environ['CODE_DIR_PATH']='test'       
sys.path.extend([["dataextraction/"]])

from sink.DefaultSparkSink import DefaultSparkSink
from ConfigHelper import ConfigHelper
import main

class helper:
    '''
        In order to mimic/mock the Line sparkdf.write.format(self.sparkloadkey).mode(self.writetype).save() for write
    '''
    def __init__(self):
        self.save_status = False
        self.write = self
    
    def format(self, sparkloadkey):
        return self
    
    def mode(self, write_type):
        return self
    
    def save(self):
        self.save_status = True

class Test_DefaultSparkSink:
    def setup_method(self):
        self.sink_obj = DefaultSparkSink("CassandraSink")
        fsConf = ConfigHelper()
        input_dict = {'CollectionName': 'last_check3'}
        self.sink_obj.init(main.session_helper,fsConf,input_dict)

    def test_init_sparksink(self):
        assert None != self.sink_obj.logger
        

    def test_write(self):
        helper_obj = helper()
        self.sink_obj.write(None , helper_obj)
        assert helper_obj.save_status