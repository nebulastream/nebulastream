import math
import random
import time

import grequests
# at the beginning of the script
from gevent import monkey

monkey.patch_all(ssl=False)


def err_handler(request, exception):
    print(exception)


# name: list of logical stream name
# n_queries: total number of queries
# change the "intv" if wanting different sinks
def generate_query(names, n_queries=100, intv=0):
    queries_per = math.ceil(n_queries / len(names))
    query_set = []
    for i in range(queries_per):
        for stream in names:
            outputFilePath = stream + "_out" + str(i + intv) + ".csv"
            query = "Query::from(\"" + stream + "\").sink(FileSinkDescriptor::create(\"" + outputFilePath + "\",\"CSV_FORMAT\",\"APPEND\")); "
            query_set.append({"userQuery": query,
                              "strategyName": "BottomUp"})
    return query_set


# sending http:post and get response
def http_post(url, request, header=None):
    if header is None:
        header = {"Content-Type": "application/json; charset=UTF-8"}
    http_req = []
    for req in request:
        http_req.append(grequests.post(url, json=req, headers=header))

    start = time.time()
    # sending in parallel, return when the last is finished
    response_list = grequests.map(http_req)
    end = time.time()
    print("Total time to sending " + str(len(http_req)) + " posts requests: USING " + str(end - start))
    return response_list


# get a field value in the response_list
def get_field(response_list, dom):
    query_id = []
    for respon in response_list:
        res = respon.json()
        #  get "queryId"
        query_id.append(res[dom])
    return query_id


# stop running queries in sample
def stop_query(url, sample):
    http_req = []
    for id in sample:
        res = {"queryId": id}
        http_req.append(grequests.delete(url, json=res))

    response_list = grequests.map(http_req)
    return response_list


# change the "dom" of get_field(response_list, dom) to "queryId" to get queryId
def generate_workload(url, n_requests=100, stable=True, ratio=0.5, iter=1, names=["ysb"], interval=1, diff=False):
    queryids = []
    for i in range(iter):
        # setting diff=True, every iteration will generate different queries
        if diff:
            query_sets = generate_query(names, n_requests, i * n_requests)
        else:
            query_sets = generate_query(names, n_requests)
        response_list = http_post(url, query_sets)
        queryids.extend(get_field(response_list, "url"))
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
            print("STOPPING: " + str(len(res)) + " queries. " + str(len(queryids)) + " are RUNNING")

    return queryids


if __name__ == '__main__':
    ids = generate_workload('https://httpbin.org/post', n_requests=100, iter=2)
