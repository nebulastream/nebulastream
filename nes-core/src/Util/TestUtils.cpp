/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Util/TestUtils.hpp>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>

namespace NES {

bool TestUtils::checkCompleteOrTimeout(QueryId queryId, uint64_t expectedResult, const std::string& restPort) {
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    uint64_t currentResult = 0;
    web::json::value json_return;
    std::string currentStatus;

    NES_DEBUG("checkCompleteOrTimeout: Check if the query goes into the Running status within the timeout");
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec && currentStatus != "RUNNING") {
        web::http::client::http_client clientProc("http://localhost:" + restPort + "/v1/nes/queryCatalog/status");
        web::uri_builder builder(("/"));
        builder.append_query(("queryId"), queryId);
        clientProc.request(web::http::methods::GET, builder.to_string())
            .then([](const web::http::http_response& response) {
                cout << "Get query status" << endl;
                return response.extract_json();
            })
            .then([&json_return, &currentStatus](const pplx::task<web::json::value>& task) {
                try {
                    NES_DEBUG("got status=" << json_return);
                    json_return = task.get();
                    currentStatus = json_return.at("status").as_string();
                } catch (const web::http::http_exception& e) {
                    NES_ERROR("error while setting return" << e.what());
                }
            })
            .wait();
        NES_DEBUG("checkCompleteOrTimeout: sleep because current status =" << currentStatus);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
    }
    NES_DEBUG("checkCompleteOrTimeout: end with status =" << currentStatus);

    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("checkCompleteOrTimeout: check result NodeEnginePtr");

        web::http::client::http_client clientProc("http://localhost:" + restPort
                                                  + "/v1/nes/queryCatalog/getNumberOfProducedBuffers");
        web::uri_builder builder(("/"));
        builder.append_query(("queryId"), queryId);
        clientProc.request(web::http::methods::GET, builder.to_string())
            .then([](const web::http::http_response& response) {
                cout << "read number of buffers" << endl;
                return response.extract_json();
            })
            .then([&json_return, &currentResult](const pplx::task<web::json::value>& task) {
                try {
                    NES_DEBUG("got #buffers=" << json_return);
                    json_return = task.get();
                    currentResult = json_return.at("producedBuffers").as_integer();
                } catch (const web::http::http_exception& e) {
                    NES_ERROR("error while setting return" << e.what());
                }
            })
            .wait();

        if (currentResult >= expectedResult) {
            NES_DEBUG("checkCompleteOrTimeout: results are correct");
            return true;
        }
        NES_DEBUG("checkCompleteOrTimeout: sleep because val=" << currentResult << " < " << expectedResult);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
    }
    NES_DEBUG("checkCompleteOrTimeout: QueryId expected results are not reached after timeout currentResult="
              << currentResult << " expectedResult=" << expectedResult);
    return false;
}

bool TestUtils::stopQueryViaRest(QueryId queryId, const std::string& restPort) {
    web::json::value json_return;

    web::http::client::http_client client("http://127.0.0.1:" + restPort + "/v1/nes/query/stop-query");
    web::uri_builder builder(("/"));
    builder.append_query(("queryId"), queryId);
    client.request(web::http::methods::DEL, builder.to_string())
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&json_return](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return");
                json_return = task.get();
            } catch (const web::http::http_exception& e) {
                NES_INFO("error while setting return");
                NES_INFO("error " << e.what());
            }
        })
        .wait();

    NES_DEBUG("stopQueryViaRest: status =" << json_return);

    return json_return.at("success").as_bool();
}

web::json::value TestUtils::startQueryViaRest(const string& queryString, const std::string& restPort) {
    web::json::value json_return;

    web::http::client::http_client clientQ1("http://127.0.0.1:" + restPort + "/v1/nes/");
    clientQ1.request(web::http::methods::POST, "/query/execute-query", queryString)
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&json_return](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return");
                json_return = task.get();
            } catch (const web::http::http_exception& e) {
                NES_INFO("error while setting return");
                NES_INFO("error " << e.what());
            }
        })
        .wait();

    NES_DEBUG("startQueryViaRest: status =" << json_return);

    return json_return;
}

bool TestUtils::addLogicalStream(const string& schemaString, const std::string& restPort) {
    web::json::value json_returnSchema;

    web::http::client::http_client clientSchema("http://127.0.0.1:" + restPort + "/v1/nes/sourceCatalog/addLogicalStream");
    clientSchema.request(web::http::methods::POST, _XPLATSTR("/"), schemaString)
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&json_returnSchema](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("set return");
                json_returnSchema = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("error while setting return");
                NES_ERROR("error " << e.what());
            }
        })
        .wait();

    NES_DEBUG("addLogicalStream: status =" << json_returnSchema);

    return json_returnSchema.at("Success").as_bool();
}

bool TestUtils::waitForWorkers(uint64_t restPort, uint16_t maxTimeout, uint16_t expectedWorkers) {
    auto baseUri = "http://localhost:" + std::to_string(restPort) + "/v1/nes/topology";
    NES_INFO("TestUtil: Executen GET request on URI " << baseUri);
    web::json::value json_return;
    web::http::client::http_client client(baseUri);
    size_t nodeNo = 0;

    for (int i = 0; i < maxTimeout; i++) {
        try {
            client.request(web::http::methods::GET)
                .then([](const web::http::http_response& response) {
                    NES_INFO("get first then");
                    return response.extract_json();
                })
                .then([&json_return](const pplx::task<web::json::value>& task) {
                    try {
                        json_return = task.get();
                    } catch (const web::http::http_exception& e) {
                        NES_ERROR("TestUtils: Error while setting return: " << e.what());
                    }
                })
                .wait();

            nodeNo = json_return.at("nodes").size();

            if (nodeNo == expectedWorkers + 1U) {
                NES_INFO("TestUtils: Expected worker number reached correctly " << expectedWorkers);
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
        } catch (const std::exception& e) {
            NES_ERROR("TestUtils: WaitForWorkers error occured " << e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
        }
    }

    NES_ERROR("E2ECoordinatorMultiWorkerTest: Expected worker number not reached correctly " << nodeNo << " but expected "
                                                                                             << expectedWorkers);
    return false;
}

}// namespace NES