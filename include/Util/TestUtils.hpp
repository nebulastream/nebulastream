/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_UTIL_TESTUTILS_HPP_
#define NES_INCLUDE_UTIL_TESTUTILS_HPP_
#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <CoordinatorRPCService.pb.h>
#include <NodeEngine/NodeEngine.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>
#include <iostream>
#include <memory>

using Seconds = std::chrono::seconds;
using Clock = std::chrono::high_resolution_clock;
using std::cout;
using std::endl;
using std::string;
namespace NES {

class NodeStats;
typedef std::shared_ptr<NodeStats> NodeStatsPtr;

/**
 * @brief this is a util class for the tests
 */
class TestUtils {
  public:
    static constexpr uint64_t timeout = 60;

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NodeEngine
     * @param queryId
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    static bool checkCompleteOrTimeout(NodeEngine::NodeEnginePtr ptr, QueryId queryId, uint64_t expectedResult) {
        if (ptr->getQueryStatistics(queryId).empty()) {
            NES_ERROR("checkCompleteOrTimeout query does not exists");
            return false;
        }
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NodeEnginePtr");
            //FIXME: handle vector of statistics properly in #977
            if (ptr->getQueryStatistics(queryId)[0]->getProcessedBuffers() == expectedResult
                && ptr->getQueryStatistics(queryId)[0]->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: NodeEnginePtr results are correct");
                return true;
            }
            NES_DEBUG("checkCompleteOrTimeout: NodeEnginePtr sleep because val="
                      << ptr->getQueryStatistics(queryId)[0]->getProcessedTuple() << " < " << expectedResult);
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: NodeEnginePtr expected results are not reached after timeout");
        return false;
    }

    /**
     * @brief This method is used for checking if the submitted query produced the expected result within the timeout
     * @param queryId: Id of the query
     * @param expectedResult: The expected value
     * @return true if matched the expected result within the timeout
     */
    static bool checkCompleteOrTimeout(QueryId queryId, uint64_t expectedResult, std::string restPort = "8081") {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        uint64_t currentResult = 0;
        web::json::value json_return;
        std::string currentStatus = "";

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
            sleep(1);
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
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: QueryId expected results are not reached after timeout currentResult="
                  << currentResult << " expectedResult=" << expectedResult);
        return false;
    }

    /**
     * @brief This method is used for stop a query
     * @param queryId: Id of the query
     * @return if stopped
     */
    static bool stopQueryViaRest(QueryId queryId, std::string restPort = "8081") {
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

    /**
     * @brief This method is used for executing a query
     * @param query string
     * @return if stopped
     */
    static web::json::value startQueryViaRest(string queryString, std::string restPort = "8081") {
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

    /**
   * @brief This method is used adding a logical stream
   * @param query string
   * @return
   */
    static bool addLogicalStream(string schemaString, std::string restPort = "8081") {
        web::json::value json_returnSchema;

        web::http::client::http_client clientSchema("http://127.0.0.1:" + restPort + "/v1/nes/streamCatalog/addLogicalStream");
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

    /**
   * @brief This method is used adding a physical stream to a logical stream
   * @param physicalStreamName
   * @param logicalStreamName
   * @return
   */
    static bool addPhysicalToLogicalStream(string physicalStreamName, string logicalStreamName, std::string restPort = "8081") {
        web::json::value json_returnSchema;

        RegisterPhysicalStreamRequest req;
        req.set_physicalstreamname(physicalStreamName);
        req.set_logicalstreamname(logicalStreamName);

        std::string msg = req.SerializeAsString();
        web::http::client::http_client addPhysicalToLogicalStreamClient(
            "http://127.0.0.1:" + restPort + "/v1/nes/streamCatalog/addPhysicalStreamToLogicalStream");
        web::json::value addJsonReturn;
        addPhysicalToLogicalStreamClient.request(web::http::methods::POST, "", msg)
            .then([](const web::http::http_response& response) {
                NES_INFO("get first then");
                return response.extract_json();
            })
            .then([&addJsonReturn](const pplx::task<web::json::value>& task) {
                try {
                    NES_INFO("addPhysicalToLogicalStream: set return");
                    addJsonReturn = task.get();
                } catch (const web::http::http_exception& e) {
                    NES_ERROR("addPhysicalToLogicalStream: error while setting return" << e.what());
                }
            })
            .wait();

        NES_DEBUG("addPhysicalToLogicalStream: status =" << json_returnSchema);

        return addJsonReturn.at("Success").as_bool();
    }

    static web::json::value getPhysicalStreamConfig(std::string physicalStreamName, std::string restPort = "8081") {
        web::json::value ret_value;

        web::http::client::http_client clientPhysicalStreamConfig(
            "http://127.0.0.1:" + restPort + "/v1/nes/streamCatalog/physicalStreamConfig?streamName=" + physicalStreamName);
        clientPhysicalStreamConfig.request(web::http::methods::GET)
            .then([](const web::http::http_response& response) {
                NES_INFO("get first then");
                return response.extract_json();
            })
            .then([&ret_value](const pplx::task<web::json::value>& task) {
                try {
                    NES_INFO("set return");
                    ret_value = task.get();
                } catch (const web::http::http_exception& e) {
                    NES_ERROR("error while setting return");
                    NES_ERROR("error " << e.what());
                }
            })
            .wait();

        NES_DEBUG("getPhysicalStreamConfig: status =" << ret_value);

        return ret_value;
    }

    /**
   * @brief This method is used getting a logical stream
   * @param query string
   * @return
   */
    static web::json::value getLogicalStreams(std::string restPort = "8081") {
        web::json::value ret_value;

        web::http::client::http_client clientLogicalStreams("http://127.0.0.1:" + restPort
                                                            + "/v1/nes/streamCatalog/allLogicalStream");
        clientLogicalStreams.request(web::http::methods::GET)
            .then([](const web::http::http_response& response) {
                NES_INFO("get first then");
                return response.extract_json();
            })
            .then([&ret_value](const pplx::task<web::json::value>& task) {
                try {
                    NES_INFO("set return");
                    ret_value = task.get();
                } catch (const web::http::http_exception& e) {
                    NES_ERROR("error while setting return");
                    NES_ERROR("error " << e.what());
                }
            })
            .wait();

        NES_DEBUG("getLogicalStream: status =" << ret_value);

        return ret_value;
    }

    /**
     * @brief This method is used for waiting till the query gets into running status or a timeout occurs
     * @param queryId : the query id to check for
     * @param queryCatalog: the catalog to look into for status change
     * @param timeoutInSec: time to wait before stop checking
     * @return true if query gets into running status else false
     */
    static bool waitForQueryToStart(QueryId queryId,
                                    QueryCatalogPtr queryCatalog,
                                    std::chrono::seconds timeoutInSec = std::chrono::seconds(timeout)) {
        NES_DEBUG("TestUtils: wait till the query " << queryId << " gets into Running status.");
        auto start_timestamp = std::chrono::system_clock::now();

        NES_DEBUG("TestUtils: Keep checking the status of query " << queryId << " untill a fixed time out");
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
            if (!queryCatalogEntry) {
                NES_ERROR("TestUtils: unable to find the entry for query " << queryId << " in the query catalog.");
                return false;
            }
            NES_TRACE("TestUtils: Query " << queryId << " is now in status " << queryCatalogEntry->getQueryStatusAsString());
            bool isQueryRunning = queryCatalogEntry->getQueryStatus() == QueryStatus::Running;
            if (isQueryRunning) {
                NES_TRACE("TestUtils: Query " << queryId << " is now in running status.");
                return isQueryRunning;
            }
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: waitForStart expected results are not reached after timeout");
        return false;
    }

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param nesWorker to NesWorker
     * @param queryId
     * @param queryCatalog
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    template<typename Predicate = std::equal_to<uint64_t>>
    static bool
    checkCompleteOrTimeout(NesWorkerPtr nesWorker, QueryId queryId, GlobalQueryPlanPtr globalQueryPlan, uint64_t expectedResult) {

        SharedQueryId sharedQueryId = globalQueryPlan->getSharedQueryIdForQuery(queryId);
        if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
            NES_ERROR("Unable to find global query Id for user query id " << queryId);
            return false;
        }

        NES_INFO("Found global query id " << sharedQueryId << " for user query " << queryId);
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesWorkerPtr");
            //FIXME: handle vector of statistics properly in #977
            auto statistics = nesWorker->getQueryStatistics(sharedQueryId);
            if (statistics.empty()) {
                NES_DEBUG("checkCompleteOrTimeout: query=" << sharedQueryId << " stats size=" << statistics.size());
                sleep(1);
                continue;
            }
            uint64_t processed = statistics[0]->getProcessedBuffers();
            if (processed >= expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct procBuffer="
                          << statistics[0]->getProcessedBuffers() << " procTasks=" << statistics[0]->getProcessedTasks()
                          << " procWatermarks=" << statistics[0]->getProcessedWatermarks());
                return true;
            }
            NES_DEBUG("checkCompleteOrTimeout: NesWorkerPtr results are incomplete procBuffer="
                      << statistics[0]->getProcessedBuffers() << " procTasks=" << statistics[0]->getProcessedTasks()
                      << " procWatermarks=" << statistics[0]->getProcessedWatermarks());
            sleep(1);
        }
        auto statistics = nesWorker->getQueryStatistics(sharedQueryId);
        uint64_t processed = statistics[0]->getProcessedBuffers();
        NES_DEBUG("checkCompleteOrTimeout: NesWorkerPtr expected results are not reached after timeout expected="
                  << expectedResult << " final result=" << processed);
        return false;
    }

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param nesCoordinator to NesCoordinator
     * @param queryId
     * @param queryCatalog
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    template<typename Predicate = std::equal_to<uint64_t>>
    static bool checkCompleteOrTimeout(NesCoordinatorPtr nesCoordinator,
                                       QueryId queryId,
                                       GlobalQueryPlanPtr globalQueryPlan,
                                       uint64_t expectedResult) {
        SharedQueryId sharedQueryId = globalQueryPlan->getSharedQueryIdForQuery(queryId);
        if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
            NES_ERROR("Unable to find global query Id for user query id " << queryId);
            return false;
        }

        NES_INFO("Found global query id " << sharedQueryId << " for user query " << queryId);
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesCoordinatorPtr");

            //FIXME: handle vector of statistics properly in #977
            auto statistics = nesCoordinator->getQueryStatistics(sharedQueryId);
            if (statistics.empty()) {
                continue;
            }

            if (statistics[0]->getProcessedBuffers() >= expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: NesCoordinatorPtr results are correct stats="
                          << statistics[0]->getProcessedBuffers() << " procTasks=" << statistics[0]->getProcessedTasks()
                          << " procWatermarks=" << statistics[0]->getProcessedWatermarks());
                return true;
            }
            NES_DEBUG("checkCompleteOrTimeout: NesCoordinatorPtr results are incomplete procBuffer="
                      << statistics[0]->getProcessedBuffers() << " procTasks=" << statistics[0]->getProcessedTasks()
                      << " expected=" << expectedResult);

            sleep(1);
        }
        //FIXME: handle vector of statistics properly in #977
        NES_DEBUG("checkCompleteOrTimeout: NesCoordinatorPtr expected results are not reached after timeout expected result="
                  << expectedResult
                  << " processedBuffer=" << nesCoordinator->getQueryStatistics(queryId)[0]->getProcessedBuffers()
                  << " processedTasks=" << nesCoordinator->getQueryStatistics(queryId)[0]->getProcessedTasks()
                  << " procWatermarks=" << nesCoordinator->getQueryStatistics(queryId)[0]->getProcessedWatermarks());
        return false;
    }

    /**
     * @brief Check if the query is been stopped successfully within the timeout.
     * @param queryId: Id of the query to be stopped
     * @param queryCatalog: the catalog containig the queries in the system
     * @return true if successful
     */
    static bool checkStoppedOrTimeout(QueryId queryId, QueryCatalogPtr queryCatalog) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkStoppedOrTimeout: check query status");
            if (queryCatalog->getQueryCatalogEntry(queryId)->getQueryStatus() == QueryStatus::Stopped) {
                NES_DEBUG("checkStoppedOrTimeout: status reached stopped");
                return true;
            }
            NES_DEBUG("checkStoppedOrTimeout: status not reached as status is="
                      << queryCatalog->getQueryCatalogEntry(queryId)->getQueryStatusAsString());
            sleep(1);
        }
        NES_DEBUG("checkStoppedOrTimeout: expected status not reached within set timeout");
        return false;
    }

    /**
   * @brief Check if the query result was produced
   * @param expectedContent
   * @param outputFilePath
   * @return true if successful
   */
    static bool checkOutputOrTimeout(string expectedContent, string outputFilePath, uint64_t customTimeout = 0) {
        std::chrono::seconds timeoutInSec;
        if (customTimeout == 0) {
            timeoutInSec = std::chrono::seconds(timeout);
        } else {
            timeoutInSec = std::chrono::seconds(customTimeout);
        }

        NES_DEBUG("using timeout=" << timeoutInSec.count());
        auto start_timestamp = std::chrono::system_clock::now();
        uint64_t found = 0;
        uint64_t count = 0;
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            sleep(1);
            found = 0;
            count = 0;
            NES_DEBUG("checkOutputOrTimeout: check content for file " << outputFilePath);
            std::ifstream ifs(outputFilePath);
            if (ifs.good() && ifs.is_open()) {
                std::vector<std::string> expectedlines = UtilityFunctions::splitWithStringDelimiter(expectedContent, "\n");
                std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
                count = std::count(content.begin(), content.end(), '\n');
                if (expectedlines.size() != count) {
                    NES_DEBUG("checkoutputortimeout: number of expected lines "
                              << expectedlines.size() << " not reached yet with " << count << " lines content=" << content);
                    continue;
                }

                if (content.size() != expectedContent.size()) {
                    NES_DEBUG("checkoutputortimeout: number of chars " << expectedContent.size()
                                                                       << " not reached yet with chars content=" << content.size()
                                                                       << " lines content=" << content);
                    continue;
                }

                for (uint64_t i = 0; i < expectedlines.size(); i++) {
                    if (content.find(expectedlines[i]) != std::string::npos) {
                        found++;
                    }
                }
                if (found == count) {
                    NES_DEBUG("all lines found final content=" << content);
                    return true;
                } else {
                    NES_DEBUG("only " << found << " lines found final content=" << content);
                }
            }
        }
        NES_ERROR("checkOutputOrTimeout: expected (" << count << ") result not reached (" << found
                                                     << ") within set timeout content");
        return false;
    }

    /**
   * @brief Check if any query result was produced
   * @param outputFilePath
   * @return true if successful
   */
    static bool checkIfOutputFileIsNotEmtpy(uint64_t minNumberOfLines, string outputFilePath, uint64_t customTimeout = 0) {
        std::chrono::seconds timeoutInSec;
        if (customTimeout == 0) {
            timeoutInSec = std::chrono::seconds(timeout);
        } else {
            timeoutInSec = std::chrono::seconds(customTimeout);
        }

        NES_DEBUG("using timeout=" << timeoutInSec.count());
        auto start_timestamp = std::chrono::system_clock::now();
        uint64_t count = 0;
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            sleep(1);
            count = 0;
            NES_DEBUG("checkIfOutputFileIsNotEmtpy: check content for file " << outputFilePath);
            std::ifstream ifs(outputFilePath);
            if (ifs.good() && ifs.is_open()) {
                std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
                count = std::count(content.begin(), content.end(), '\n');
                if (count < minNumberOfLines) {
                    NES_DEBUG("checkIfOutputFileIsNotEmtpy: number of min lines " << minNumberOfLines << " not reached yet with "
                                                                                  << count << " lines content=" << content);
                    continue;
                } else {
                    NES_DEBUG("at least" << minNumberOfLines << " are found in content=" << content);
                    return true;
                }
            }
        }
        NES_ERROR("checkIfOutputFileIsNotEmtpy: expected (" << count << ") result not reached (" << minNumberOfLines
                                                            << ") within set timeout content");
        return false;
    }

    /**
  * @brief Check if the query result was produced
  * @param expectedContent
  * @param outputFilePath
  * @return true if successful
  */
    template<typename T>
    static bool checkBinaryOutputContentLengthOrTimeout(uint64_t expectedNumberOfContent,
                                                        string outputFilePath,
                                                        uint64_t testTimeout = timeout) {
        auto timeoutInSec = std::chrono::seconds(testTimeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            sleep(1);
            NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout: check content for file " << outputFilePath);
            std::ifstream ifs(outputFilePath);
            if (ifs.good() && ifs.is_open()) {
                NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout:: file " << outputFilePath << " open and good");
                std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
                // check the length of the output file
                ifs.seekg(0, ifs.end);
                auto length = ifs.tellg();
                ifs.seekg(0, ifs.beg);

                // read the binary output as a vector of T
                auto* buff = reinterpret_cast<char*>(malloc(length));
                ifs.read(buff, length);
                std::vector<T> currentContent(reinterpret_cast<T*>(buff), reinterpret_cast<T*>(buff) + length / sizeof(T));
                uint64_t currentContentSize = currentContent.size();

                ifs.close();
                free(buff);

                if (expectedNumberOfContent != currentContentSize) {
                    if (currentContentSize > expectedNumberOfContent) {
                        NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout:: content is larger than expected result");
                        return false;
                    }

                    NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout:: number of expected lines "
                              << expectedNumberOfContent << " not reached yet with " << currentContent.size()
                              << " lines content=" << content);
                    continue;
                } else {
                    NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout: number of content in output file match expected "
                              "number of content");
                    return true;
                }
            }
        }
        NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout:: expected result not reached within set timeout content");
        return false;
    }

    /**
   * @brief Check if a outputfile is created
   * @param expectedContent
   * @param outputFilePath
   * @return true if successful
   */
    static bool checkFileCreationOrTimeout(string outputFilePath) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            sleep(1);
            NES_DEBUG("checkFileCreationOrTimeout: for file " << outputFilePath);
            std::ifstream ifs(outputFilePath);
            if (ifs.good() && ifs.is_open()) {
                return true;
            }
        }
        NES_DEBUG("checkFileCreationOrTimeout: expected result not reached within set timeout");
        return false;
    }

    static bool waitForWorkers(uint64_t restPort, uint16_t maxTimeout, uint16_t expectedWorkers) {
        auto baseUri = "http://localhost:" + std::to_string(restPort) + "/v1/nes/topology";
        NES_INFO("TestUtil: Executen GET request on URI " << baseUri);
        web::json::value json_return;
        web::http::client::http_client client(baseUri);
        size_t nodeNo;

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

                if (nodeNo == expectedWorkers + 1u) {
                    NES_INFO("TestUtils: Expected worker number reached correctly " << expectedWorkers);
                    return true;
                } else {
                    sleep(1);
                }
            } catch (const std::exception& e) {
                NES_ERROR("TestUtils: WaitForWorkers error occured " << e.what());
                sleep(1);
            }
        }

        NES_ERROR("E2ECoordinatorMultiWorkerTest: Expected worker number not reached correctly " << nodeNo << " but expected "
                                                                                                 << expectedWorkers);
        return false;
    }
};
}// namespace NES
#endif//NES_INCLUDE_UTIL_TESTUTILS_HPP_
