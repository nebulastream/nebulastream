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
#include <NodeEngine/NodeEngine.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
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
    static const uint64_t timeout = 30;//TODO:increase it again to 30

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NodeEngine
     * @param queryId
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    static bool checkCompleteOrTimeout(NodeEnginePtr ptr, QueryId queryId, uint64_t expectedResult) {
        if (ptr->getQueryStatistics(queryId).empty()) {
            NES_ERROR("checkCompleteOrTimeout query does not exists");
            return false;
        }
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        auto now = start_timestamp;
        while ((now = std::chrono::system_clock::now()) < start_timestamp + timeoutInSec) {
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
            clientProc.request(web::http::methods::GET, _XPLATSTR("/"), queryId)
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
            clientProc.request(web::http::methods::GET, _XPLATSTR("/"), queryId)
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
        client.request(web::http::methods::DEL, _XPLATSTR("/"), queryId)
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
     * @brief This method is used for waiting till the query gets into running status or a timeout occurs
     * @param queryId : the query id to check for
     * @param queryCatalog: the catalog to look into for status change
     * @return true if query gets into running status else false
     */
    static bool waitForQueryToStart(QueryId queryId, QueryCatalogPtr queryCatalog) {
        NES_DEBUG("TestUtils: wait till the query " << queryId << " gets into Running status.");
        auto timeoutInSec = std::chrono::seconds(timeout);
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
    static bool checkCompleteOrTimeout(NesWorkerPtr nesWorker, QueryId queryId, GlobalQueryPlanPtr globalQueryPlan,
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
    static bool checkCompleteOrTimeout(NesCoordinatorPtr nesCoordinator, QueryId queryId, GlobalQueryPlanPtr globalQueryPlan,
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
                      << statistics[0]->getProcessedBuffers() << " procTasks=" << statistics[0]->getProcessedTasks());

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
    static bool checkOutputOrTimeout(string expectedContent, string outputFilePath) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            sleep(1);
            NES_DEBUG("checkOutputOrTimeout: check content for file " << outputFilePath);
            std::ifstream ifs(outputFilePath);
            if (ifs.good() && ifs.is_open()) {
                NES_DEBUG("checkOutputOrTimeout: file " << outputFilePath << " open and good");
                std::vector<std::string> expectedLines = UtilityFunctions::split(expectedContent, '\n');
                std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
                int count = std::count(content.begin(), content.end(), '\n');
                if (expectedLines.size() != count) {
                    NES_DEBUG("checkOutputOrTimeout: number of expected lines "
                              << expectedLines.size() << " not reached yet with " << count << " lines content=" << content);
                    continue;
                }

                uint64_t found = 0;
                for (uint64_t i = 0; i < expectedLines.size(); i++) {
                    if (content.find(expectedLines[i]) != std::string::npos) {
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
        NES_DEBUG("checkOutputOrTimeout: expected result not reached within set timeout content");
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

    static TopologyNodePtr registerTestNode(uint64_t id, std::string address, int cpu, NodeStats nodeProperties,
                                            PhysicalStreamConfigPtr streamConf, NodeType type, StreamCatalogPtr streamCatalog,
                                            TopologyPtr topology) {
        TopologyNodePtr nodePtr;
        if (type == NodeType::Sensor) {
            NES_DEBUG("CoordinatorService::registerNode: register sensor node");
            nodePtr = TopologyNode::create(id, address, 4000, 4002, cpu);

            NES_DEBUG("try to register sensor phyName=" << streamConf->getPhysicalStreamName() << " logName="
                                                        << streamConf->getLogicalStreamName() << " nodeID=" << nodePtr->getId());

            //check if logical stream exists
            if (!streamCatalog->testIfLogicalStreamExistsInSchemaMapping(streamConf->getLogicalStreamName())) {
                NES_ERROR("Coordinator: error logical stream" << streamConf->getLogicalStreamName()
                                                              << " does not exist when adding physical stream "
                                                              << streamConf->getPhysicalStreamName());
                throw Exception("logical stream does not exist " + streamConf->getLogicalStreamName());
            }

            SchemaPtr schema = streamCatalog->getSchemaForLogicalStream(streamConf->getLogicalStreamName());

            DataSourcePtr source;
            string sourceType = streamConf->getSourceType();
            if (sourceType != "CSVSource" && sourceType != "DefaultSource" && sourceType != "OPCSource" && sourceType != "ZMQSource" && sourceType != "KafkaSource" && sourceType != "BinarySource" && sourceType != "SenseSource" && sourceType != "NetworkSource" && sourceType != "AdaptiveSource") {
                NES_ERROR("Coordinator: error source type " << sourceType << " is not supported");
                NES_ERROR("CoordinatorEngine::registerNode: available options: OPCSource, ZMQSource, KafkaSource, TestSource, BinarySource, SenseSource, NetworkSource, AdaptiveSource");
                throw Exception("Coordinator: error source type " + sourceType + " is not supported");
            }

            StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, nodePtr);

            bool success = streamCatalog->addPhysicalStream(streamConf->getLogicalStreamName(), sce);
            if (!success) {
                NES_ERROR("Coordinator: physical stream " << streamConf->getPhysicalStreamName()
                                                          << " could not be added to catalog");
                throw Exception("Coordinator: physical stream " + streamConf->getPhysicalStreamName()
                                + " could not be added to catalog");
            }

        } else if (type == NodeType::Worker) {
            NES_DEBUG("CoordinatorService::registerNode: register worker node");
            nodePtr = TopologyNode::create(id, address, 4004, 4006, cpu);
        } else {
            NES_ERROR("CoordinatorService::registerNode: type not supported " << type);
            assert(0);
        }
        assert(nodePtr);

        if (nodeProperties.IsInitialized()) {
            nodePtr->setNodeStats(nodeProperties);
        }

        const TopologyNodePtr rootNode = topology->getRoot();

        if (rootNode == nodePtr) {
            NES_DEBUG("CoordinatorService::registerNode: tree is empty so this becomes new root");
            topology->setAsRoot(nodePtr);
        } else {
            NES_DEBUG("CoordinatorService::registerNode: add link to root node " << rootNode);
            topology->addNewPhysicalNodeAsChild(rootNode, nodePtr);
        }
        return nodePtr;
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

                if (nodeNo == expectedWorkers + 1) {
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
