import math
import random
import time
from datetime import datetime
from typing import List, Dict, Optional

import grequests
import requests
from requests import Response

start_time: str = '{date:%Y%m%d_%H%M%S}'.format(date=datetime.now())


def generate_query(base_query: str, n_queries: int = 100, n_iter: int = 0) -> List[Dict[str, str]]:
    """
    Generate queries for submitting, we use a default query to start with
    :param names: list of logical stream name
    :param n_queries: total number of queries
    :param n_iter: nth iteration
    :return:
    """
    query_set = []
    for i in range(n_queries):
        output_file_path = f"{start_time}_{n_iter}_{i}.csv"
        query = f"{base_query}.sink(FileSinkDescriptor::create(\"{output_file_path}\",\"CSV_FORMAT\",\"APPEND\")); "
        query_set.append({"userQuery": query,
                          "strategyName": "BottomUp"})
    return query_set


# sending http:post and get response
def submit_query(base_url: str, request: List[Dict[str, str]], header=None):
    if header is None:
        header = {"Content-Type": "application/json; charset=UTF-8"}
    submit_query_url = f"{base_url}/query/execute-query"
    http_req = []
    for req in request:
        http_req.append(grequests.post(submit_query_url, json=req, headers=header))

    start = time.time()
    # sending in parallel, return when the last is finished
    response_list = grequests.map(http_req)
    end = time.time()
    print("Total time to sending " + str(len(http_req)) + " posts requests: USING " + str(end - start))
    return response_list


# get a field value in the response_list
def get_query_id(responses: List[Response]):
    query_id = []
    for response in responses:
        query_id.append(response.json()["queryId"])
    return query_id


# stop running queries in sample
def stop_query(base_url: str, query_ids: List[int]):
    stop_query_url = f"{base_url}/queryCatalog/query"
    http_req = []
    for query_id in query_ids:
        http_req.append(grequests.delete(stop_query_url, json={"queryId": query_id}))

    response_list = grequests.map(http_req)
    return response_list


def number_of_queries_waiting_to_run(base_url: str) -> int:
    valid_status_codes = [200, 204]
    status_url = f"{base_url}/queryCatalog/queries"
    registered_response = requests.get(status_url, json={"status": "Registered"})
    scheduling_response = requests.get(status_url, json={"status": "Scheduling"})
    if registered_response.status_code not in valid_status_codes or scheduling_response.status_code not in valid_status_codes:
        Exception(
            f"Registered Response Code {registered_response.status_code}. Scheduling Response Code {scheduling_response.status_code}")
    registered_count = 0
    scheduled_count = 0
    if registered_response.status_code == 200:
        registered_count = len(_filter_none(registered_response.json()))
    if scheduling_response.status_code == 200:
        scheduled_count = len(_filter_none(scheduling_response.json()))
    return registered_count + scheduled_count


def _filter_none(response: List[Optional[str]]) -> List[str]:
    return [r for r in response if r is not None]


def generate_workload(base_url: str, base_query: str, n_requests: int = 100, stable: bool = True, ratio: float = 0.5,
                      niter: int = 1):
    queryids = []
    for i in range(niter):
        print(f"Running iteration {i + 1}")
        query_sets = generate_query(base_query, n_requests, i)
        response_list = submit_query(base_url, query_sets)
        recent_query_ids = get_query_id(response_list)
        while number_of_queries_waiting_to_run(base_url) > 0:
            print(f"Waiting until submitted queries are marked as running.")
            time.sleep(1)
        if not stable:
            random.shuffle(queryids)
            if ratio > 1:
                ratio = 1
            # the number of queries to delete
            n_stop = math.ceil(ratio * len(queryids))
            stop_set = queryids[: n_stop]
            res = stop_query(base_url, stop_set)
            # still RUNNING queries
            queryids = queryids[n_stop:]
            stopped_query_ids = ",".join([str(qid) for qid in stop_set])
            print(f"STOPPED Queries {stopped_query_ids}. {len(queryids)} queries are still RUNNING")
        queryids.extend(recent_query_ids)
,

if __name__ == '__main__':
    NES_BASE_URL = "http://localhost:8081/v1/nes"
    BASE_QUERY = "Query::from(\"" + "ysb" + "\")"
    generate_workload(NES_BASE_URL, BASE_QUERY, stable=True, n_requests=10, niter=10)
