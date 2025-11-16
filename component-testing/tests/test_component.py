import requests
import pytest

BASE_URL = "http://localhost:32001"

def get_base_job_payload():
    return {
        "source": {
            "InfluxSource": {
            "query": "from(bucket:\"UEData\") |> range(start: 0, stop: now()) |> filter(fn: (r) => r._measurement == \"liveCell\") |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")"
            }
        },
        "transform": [
            {
            "operation": "SQLTransform",
            "FeatureList": "pdcpBytesDl,pdcpBytesUl",
            "SQLFilter": "nrCellIdentity = '\''c1/B13'\''"
            }
        ],
        "sink": {
            "CassandraSink": {
            "CollectionName": "base1_52"
            }
        },
        "trainingjob_id": "52",
        "influxdb_info": {
            "host": "mock-influxdb.traininghost",
            "port": "8080",
            "bucket": "UEData",
            "token": "xxx",
            "db_org": "primary",
            "source_name": ""
        }
    }
    
def submit_data_extraction_job(trainingjob_id):
    '''
        Helper function to submit data-extraction joband returns the task_id
    '''
    url = f"{BASE_URL}/feature-groups"
    payload = get_base_job_payload()
    payload["sink"]["CassandraSink"]["CollectionName"] = f"base1_{trainingjob_id}"
    payload["trainingjob_id"] = trainingjob_id
    r = requests.post(url=url, json=payload)
    assert r.status_code == 200, f"Job Submission didn't returned 200 but returned {r.status_code}"
    task_id = r.json().get("featurepath")
    return task_id


def test_data_extraction_job_submission_and_status_retrieval():
    '''
        The following test verifies that a data-extraction job can be successfully submitted and that its task status is retrievable via the corresponding API.
    '''
    # Submit the Data-extraction job
    trainingjob_id = "100" 
    task_id = submit_data_extraction_job(trainingjob_id)
    
    # Check job Status
    task_status_url = f"{BASE_URL}/task-status/{task_id}"
    r = requests.get(url=task_status_url)
    response_dict = r.json()
    assert r.status_code == 200, f"Task Retrieval didn't returned 200, but returned {r.status_code}"
    assert response_dict.get("trainingjob_id") == trainingjob_id, "Trainingjob_id submitted and retrieved doesn't match"