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

sys.path.extend([["dataextraction/"]])
from ConfigHelper import ConfigHelper
from tmgr_logger import TMLogger
from sink.CassandraSink import CassandraSink


def test_initWithCodePath():
    load_dotenv('test/test_env.env')
    os.environ['CODE_DIR_PATH']='test'
    fsConf = ConfigHelper()
    assert None != fsConf

def test_getClassConfig():
    load_dotenv('test/test_env.env')
    os.environ['CODE_DIR_PATH']='test'
    fsConf = ConfigHelper()   
    assert None != fsConf.getClassConfig(CassandraSink("sink"))

def test_getEnvConfig():
    load_dotenv('test/test_env.env')
    os.environ['CODE_DIR_PATH']='test'
    assert None != ConfigHelper().getEnvConfig()

def test_getLogLevel():
    load_dotenv('test/test_env.env')
    os.environ['CODE_DIR_PATH']='test'
    assert None != ConfigHelper().getLogLevel()

def test_getLogger():
    load_dotenv('test/test_env.env')
    os.environ['CODE_DIR_PATH']='test'
    assert None != ConfigHelper().getLogger()

def test_getFsHost():
    load_dotenv('test/test_env.env')
    os.environ['CODE_DIR_PATH']='test'
    assert None != ConfigHelper().getFsHost()

def test_getFsPort():
    load_dotenv('test/test_env.env')
    os.environ['CODE_DIR_PATH']='test'
    assert None != ConfigHelper().getFsPort()