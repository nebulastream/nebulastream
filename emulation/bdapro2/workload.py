import math
import random
import time
from typing import List, Dict

import grequests
from requests import Response


def generate_query(names: List[str], n_queries: int = 100, n_iter: int = 0) -> List[Dict[str, str]]:
    """
    Generate queries for submitting, we use a default query to start with
    :param names: list of logical stream name
    :param n_queries: total number of queries
    :param n_iter: nth iteration
    :return:
    """
    queries_per = math.ceil(n_queries / len(names))
    query_set = []
    for i in range(queries_per):
        for stream in names:
            output_file_path = f"{stream}_out_{n_iter}_{i}.csv"
            query = "Query::from(\"" + stream + "\").sink(FileSinkDescriptor::create(\"" + output_file_path + "\",\"CSV_FORMAT\",\"APPEND\")); "
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
def stop_query(base_url: str, sample):
    stop_query_url = f"{base_url}/queryCatalog/query"
    http_req = []
    for id in sample:
        http_req.append(grequests.delete(stop_query_url, json={"queryId": id}))

    response_list = grequests.map(http_req)
    return response_list


def generate_workload(url, n_requests=100, stable=True, ratio=0.5, iter=1, names=["ysb"], interval=1, diff=False):
    queryids = []
    for i in range(iter):
        print(f"Running iteration {i + 1}")
        # setting diff=True, every iteration will generate different queries
        if diff:
            query_sets = generate_query(names, n_requests, i)
        else:
            query_sets = generate_query(names, n_requests)
        response_list = submit_query(url, query_sets)
        queryids.extend(get_query_id(response_list))
        if stable:
            time.sleep(interval)
        else:
            random.shuffle(queryids)
            if ratio > 1:
                ratio = 1
            # the number of queries to delete
            n_stop = math.ceil(ratio * len(queryids))
            stop_set = queryids[: n_stop]
            res = stop_query(url, stop_set)
            # still RUNNING queries
            queryids = queryids[n_stop:]
            stopped_query_ids = ",".join([str(qid) for qid in stop_set])
            print(f"STOPPED Queries {stopped_query_ids}. {len(queryids)} are still RUNNING")

    return queryids


if __name__ == '__main__':
    NES_BASE_URL = "http://localhost:8081/v1/nes"
    ids = generate_workload(NES_BASE_URL, stable=False, n_requests=2, iter=2)
