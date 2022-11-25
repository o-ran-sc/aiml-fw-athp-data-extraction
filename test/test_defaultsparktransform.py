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
sys.path.extend(["dataextraction/"])
from transform.DefaultSparkTransform import DefaultSparkTransform
from ConfigHelper import ConfigHelper
import main

def test_init_default_spark_transform():
    fsConf = ConfigHelper()
    trans_obj = DefaultSparkTransform("transform")
    input_dict = {'CollectionName': 'last_check3'}
    trans_obj.init(main.session_helper,fsConf,input_dict)
    trans_obj.transform(main.session_helper, None) # Remove this when Transform is implemented properly, and write another test_case for transform then!!
    assert None != trans_obj

