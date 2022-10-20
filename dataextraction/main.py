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
Module Rest interface to access
Feature Engineering Pipeline
"""
import traceback
import datetime
import json
import threading
import queue

   
import jsonpickle
from flask import Flask
from flask_restful import  request
from flask_api import status
from SparkHelper import sparkSessionManager
from ConfigHelper import ConfigHelper
from FeatureEngineeringFactory import FeatureEngineeringFactory

fsConf = ConfigHelper()
logger = fsConf.getLogger()
session_helper = sparkSessionManager()
factory = FeatureEngineeringFactory(session_helper)
tasks = queue.Queue()
task_map = {}

class task():
    """
    Task Class
    """
    def __init__(self, task, task_status ):
        """
        TaskName
        """
        self.task = task
        self.status = task_status
        self.task_error = None
app = Flask(__name__)
@app.route('/feature-groups', methods=['POST'])
def post_handle():
    """
        Creates and Executes a DataPipeline

            Args:
                source (dictionary) : This is the Data Source for
                the Data Pipeline e.g. InfluxDB
                transformer (dictionary) : This is the Transformation that
                is required for feature modification e.g. SQL,VectorAssembler
                sink (dictionary): Specific feature filter values for each training job is defined
            Return: json dict: denoting the result of data extraction task
            status: HTTP status 200 or 500
            Raises:
            Exception:
            If supplied "trainingjob_name",list of features are empty,
            If one of more suplied feature does not exist in data lake
            If Data lake or feature store connection is down
    """
    start_time = datetime.datetime.now()
    logger.debug(str(start_time) +" Call Started")
    request_json = request.get_json(force = True)
    logger.debug("Got json list: "+str( request_json))
    response_code = status.HTTP_200_OK
    try:
        task_id = str(request_json["sink"]["CassandraSink"]["CollectionName"])
        api_result_msg = "/task-status/"+task_id
        logger.debug("Generated ID"+task_id)
        tasks.put(task_id)
        task_map[task_id] = task(request_json ,"Accepted")
        logger.debug("Generated ID"+task_id)
    except Exception as exc:
        api_result_msg = str(exc)
        response_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        logger.error('ERROR in data extration'+str(api_result_msg))
        logger.error(str(traceback.format_exc()))
    
    response = app.response_class(response=json.\
            dumps(\
        { "trainingjob_name":request_json["sink"]["CassandraSink"]["CollectionName"],\
        "result" : api_result_msg }),\
        status= response_code,mimetype='application/json')
    end_time = datetime.datetime.now()
    logger.info(str(end_time-start_time)+' API call finished')
    return response
@app.route('/task-status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """
        Return Task Status
    """
    try:
        taskstatus = task_map[task_id].status
        response_code = status.HTTP_200_OK
        api_result_msg = "Data Pipeline Execution "+taskstatus
        if taskstatus == "Error":
            response_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            api_result_msg =  task_map[task_id].error
    except Exception as exc:
        response_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        api_result_msg = str(exc)
        taskstatus = "Error"

    response = app.response_class(response=json.dumps(
        { "task_status":taskstatus,"result" : api_result_msg }),
        status= response_code,mimetype='application/json')
    return response
@app.route('/task-statuses', methods=['GET'])
def get_task_statuses():
    """
        Return Task Status
    """
    try:
        response_code = status.HTTP_200_OK
        response = jsonpickle.encode(task_map)
    except Exception as exc:
        response_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        response = str(exc)

    response = app.response_class(response,
        status= response_code,mimetype='application/json')
    return response
@app.route('/delete-task-status/<task_id>', methods=['DELETE'])
def delete_task_status(task_id):
    """
        delete Task Status
    """
    response_code = status.HTTP_200_OK
    try:
        api_result_msg =  jsonpickle.encode(task_map[task_id])
        task_map.pop(task_id)
   #pylint diable=W0703
    except Exception as exc:
        response_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        api_result_msg = str(exc)
    response = app.response_class(response=json.dumps({ "trainingjob_name": task_id,"result" : api_result_msg }),        status= response_code,mimetype='application/json')
    return response
    
def async_code_worker():
    """
    AsyncCode Worker
    Infinite loop which will retrive and process tasks assigned for executing data extraction
    """
    while True:
        try:
            start_time = datetime.datetime.now()
            logger.debug(str(start_time) +"Feature Engineering Pipeline Started")
            task_id = tasks.get()
            request_json = task_map[task_id].task
            task_map[task_id].status = "In Progress"
            source_dict = request_json["source"]
            transform_dict = request_json["transform"]
            sink_dict = request_json["sink"]
            c_key = str(source_dict)+str(transform_dict)+str(sink_dict)
            logger.debug(c_key)
            feature_engineering_pipeline = factory.getBatchPipeline(source_dict, transform_dict, sink_dict, c_key)
            session = session_helper.getSession()
            feature_engineering_pipeline.loadData(session)
            feature_engineering_pipeline.transformData(session)
            feature_engineering_pipeline.writeData(session)
            session_helper.stop()
            task_map[task_id].status = "Completed"
            tasks.task_done()
            end_time = datetime.datetime.now()
            logger.debug(str(end_time) +"Feature Engineering Pipline Ended")
        except Exception as exc:
            session_helper.stop()
            traceback.print_exc()
            logger.error('ERROR in processing task id:'+task_id+" Error:"+str(exc))
            api_result_msg = str(exc)
            task_map[task_id].status = "Error"
            task_map[task_id].error = api_result_msg
if __name__ == "__main__":
    print("******Initiaizing feature store API ******" )
    threading.Thread(target=async_code_worker, daemon=True).start()
    app.run(host=fsConf.getFsHost(), port = fsConf.getFsPort(), debug=True)
