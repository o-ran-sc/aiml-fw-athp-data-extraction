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

# In order to import dataextraction functions
sys.path.extend(["dataextraction/"])

from Pipeline import Pipeline
from FeatureEngineeringFactory import FeatureEngineeringFactory
from SparkHelper import sparkSessionManager



class helper:
    '''Helper class to Mimic data Load, Transform and Sink'''
    def __init__(self):
        pass

    def load(self, sparksession):
        return 'Data Load Completed'
    
    def transform(self, sparksession, df_list):
        return 'Data Transform Completed'
    
    def write(self, sparksession, transform_df_list):
        return 'Data Written to Sink'

class Test_Pipeline:
    def setup_method(self):
        api_json = {'source': {'InfluxSource': {'query': 'from(bucket:"UEData") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "liveCell") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}}, 'transform': [{'operation': 'SQLTransform', 'FeatureList': '*', 'SQLFilter': ''}], 'sink': {'CassandraSink': {'CollectionName': 'last_check4'}}}
        load_dotenv('test/test_env.env')
        os.environ['CODE_DIR_PATH'] = 'test'
        session_helper = sparkSessionManager()
        factory = FeatureEngineeringFactory(session_helper)
        (source_dict, transform_dict, sink_dict) = (api_json['source'], api_json['transform'], api_json['sink'])
        self.obj = factory.getBatchPipeline(source_dict, transform_dict, sink_dict, str(source_dict) + str(transform_dict) + str(sink_dict))
        self.spark_session = session_helper

    
    def test_init_pipeline(self):
        assert self.obj != None, 'Pipeline Object Creation, Failed'

        

    def test_loadData(self):
        assert self.obj != None, 'Pipeline Object Creation, Failed'
        self.obj.sources[0] = helper()
        self.obj.loadData(self.spark_session )
        assert self.obj.spark_dflist == 'Data Load Completed', 'Data Load Failed'

    def test_transformData(self):
        self.obj.transformers[0] = helper()
        self.obj.transformData(self.spark_session)

        assert self.obj.transformed_df == 'Data Transform Completed', 'Data Transform Failed'

    def test_transformDataWithNoTransform(self):
        self.obj.transformers = None
        self.obj.spark_dflist = 'Data Transform Completed'
        self.obj.transformData(self.spark_session)
        assert self.obj.transformed_df == 'Data Transform Completed', 'Data Transform Failed When No Transformer is specified'

    def test_writeData(self):
        self.obj.sinks[0] = helper()
        self.obj.writeData(self.spark_session)
        assert True


    def test_execute(self):
        self.obj.sources[0] = helper()
        self.obj.transformers[0] = helper()
        self.obj.sinks[0] = helper()
        self.obj.execute(self.spark_session)
        assert True
