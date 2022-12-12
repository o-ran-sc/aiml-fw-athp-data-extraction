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
import json
from mock import patch
from flask_api import status

load_dotenv('test/test_env.env') # Loading Env Variables 
os.environ['CODE_DIR_PATH']='test' # Telling ConfigHelper to look into test folder for log_config.yaml     
sys.path.extend(["dataextraction/"]) # In order to import dataextraction functions


import main
from SparkHelper import SparkSessionManager
from Pipeline import Pipeline
from main import Task



def empyty_tasks():
    while(main.tasks.qsize() > 0):
        task_id = main.tasks.get()
        main.tasks.task_done()

class Test_feature_groups:
    def setup_method(self):
        self.client = main.app.test_client(self)
        

    def test_feature_group(self):
        request_json = {'source': {'InfluxSource': {'query': 'from(bucket:"UEData") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "liveCell") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}}, 'transform': [{'operation': 'SQLTransform', 'FeatureList': '*', 'SQLFilter': ''}], 'sink': {'CassandraSink': {'CollectionName': 'last_check3'}}}
        response = self.client.post("/feature-groups", data=json.dumps(request_json), content_type='application/json')
        empyty_tasks()
        assert response.content_type ==  'application/json'
        assert response.status_code ==  status.HTTP_200_OK

    #Error test
    def test_negative_feature_groups(self):
        request_json = {'source': {'InfluxSource': {'query': 'from(bucket:"UEData") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "liveCell") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}}, 'transform': [{'operation': 'SQLTransform', 'FeatureList': '*', 'SQLFilter': ''}]}
        response = self.client.post("/feature-groups", data=json.dumps(request_json), content_type='application/json') 
        assert response.content_type ==  'text/html; charset=utf-8'
        


class Test_task_status:
    def setup_method(self):
        self.client = main.app.test_client(self)
     
    def test_task_status(self):  
        request_json = {'source': {'InfluxSource': {'query': 'from(bucket:"UEData") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "liveCell") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}}, 'transform': [{'operation': 'SQLTransform', 'FeatureList': '*', 'SQLFilter': ''}], 'sink': {'CassandraSink': {'CollectionName': 'last_check3'}}}
        main.task_map["unittest_task"] = Task(request_json ,"Accepted")
        response = self.client.get("/task-status/unittest_task")
        main.task_map.clear()
        assert response.content_type ==  'application/json'
        assert response.status_code ==  status.HTTP_200_OK

    #Error test
    def test_negative_task_status(self):  
        request_json = {'source': {'InfluxSource': {'query': 'from(bucket:"UEData") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "liveCell") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}}, 'transform': [{'operation': 'SQLTransform', 'FeatureList': '*', 'SQLFilter': ''}], 'sink': {'CassandraSink': {'CollectionName': 'last_check3'}}}
        main.task_map["unittest_task"] = Task(request_json ,"Error")
        response = self.client.get("/task-status/unittest_task")
        main.task_map.clear()
       
        assert str(response.status) == "500 INTERNAL SERVER ERROR", "Test_negative_task_status doesn't retured response code 500"

class Test_task_statuses:
    def setup_method(self):
        self.client = main.app.test_client(self)
     
    def test_task_statuses(self):  
        response = self.client.get("/task-statuses")
        assert None != response


class Test_delete_task_status:
    def setup_method(self):
        self.client = main.app.test_client(self)

    def test_delete_task_status(self):
        request_json = {'source': {'InfluxSource': {'query': 'from(bucket:"UEData") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "liveCell") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}}, 'transform': [{'operation': 'SQLTransform', 'FeatureList': '*', 'SQLFilter': ''}], 'sink': {'CassandraSink': {'CollectionName': 'last_check3'}}}
        main.task_map["unittest_task"] = Task(request_json ,"Accepted")
        response = self.client.delete("/delete-task-status/unittest_task")
        main.task_map.clear()
        assert response.content_type ==  'application/json'
        assert response.status_code ==  status.HTTP_200_OK

    #error scenario
    def test_negative_delete_task_status(self):
        response = self.client.delete("/delete-task-status/unittest_task")
        assert response.content_type ==  'application/json'
        assert response.status_code ==  status.HTTP_500_INTERNAL_SERVER_ERROR

class Test_async_code_worker:
    def setup_method(self):
        self.client = main.app.test_client(self)
    
    @patch('main.session_helper.get_session')
    @patch('main.factory.get_batch_pipeline')
    def test_negative_async_code_worker_1(self,mock1,mock2):
        
        main.infinte_loop_config["infinte_run"]= "True"
        main.infinte_loop_config["unit_test_mode"]= "True"
        request_json = {'source': {'InfluxSource': {'query': 'from(bucket:"UEData") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "liveCell") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}}, 'transform': [{'operation': 'SQLTransform', 'FeatureList': '*', 'SQLFilter': ''}], 'sink': {'CassandraSink': {'CollectionName': 'last_check3'}}}
        response = self.client.post("/feature-groups", data=json.dumps(request_json), content_type='application/json')
        main.async_code_worker()
        assert main.infinte_loop_config["infinte_run"] == "False"
        
    
    #error
    def test_negative_async_code_worker_2(self):
        
        main.infinte_loop_config["infinte_run"]= "True"
        main.infinte_loop_config["unit_test_mode"]= "True"
        request_json = {'source': {'InfluxSource': {'query': 'from(bucket:"UEData") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "liveCell") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}}, 'transform': [{'operation': 'SQLTransform', 'FeatureList': '*', 'SQLFilter': ''}], 'sink': {'CassandraSink': {'CollectionName': 'last_check3'}}}
        response = self.client.post("/feature-groups", data=json.dumps(request_json), content_type='application/json')
        main.async_code_worker()
        assert main.infinte_loop_config["infinte_run"] == "False"

    #error with Cassandra Source
    def test_negative_async_code_worker_3(self):

        main.infinte_loop_config["infinte_run"]= "True"
        main.infinte_loop_config["unit_test_mode"]= "True"
        request_json = {'source': {'CassandraSource': {'query': 'from(bucket:"UEData") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == "liveCell") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}}, 'transform': [{'operation': 'SQLTransform', 'FeatureList': '*', 'SQLFilter': ''}], 'sink': {'CassandraSink': {'CollectionName': 'last_check3'}}}
        response = self.client.post("/feature-groups", data=json.dumps(request_json), content_type='application/json')
        main.async_code_worker()
        assert main.infinte_loop_config["infinte_run"] == "False"