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

# In order to import dataextraction functions
sys.path.extend([["dataextraction/"]])

from tmgr_logger import TMLogger

def test_get_logger():
    load_dotenv('test/test_env.env') 
    os.environ['CODE_DIR_PATH']='test'
    tm_logger = TMLogger("test/log_config.yaml")
    assert None != tm_logger.get_logger

def test_get_logLevel():
    load_dotenv('test/test_env.env') 
    os.environ['CODE_DIR_PATH']='test'
    tm_logger = TMLogger("test/log_config.yaml")
    assert None != tm_logger.get_logLevel

def test_init_withWrongFile():
    load_dotenv('test/test_env.env') 
    os.environ['CODE_DIR_PATH']='test'
    with pytest.raises(Exception) as exc:
        tm_logger = TMLogger("bad_log_config.yaml")
        
    assert "error opening yaml config file" in str(exc.value)