#ifndef NES_INCLUDE_UTIL_TESTUTILS_HPP_
#define NES_INCLUDE_UTIL_TESTUTILS_HPP_
#include <API/QueryId.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <chrono>
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
    static const size_t timeout = 60;//TODO:increase it again to 30

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NodeEngine
     * @param queryId
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    static bool checkCompleteOrTimeout(NodeEnginePtr ptr, QueryId queryId, size_t expectedResult) {
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
                NES_DEBUG("checkCompleteOrTimeout: results are correct");
                return true;
            }
            NES_DEBUG(
                "checkCompleteOrTimeout: sleep because val=" << ptr->getQueryStatistics(queryId)[0]->getProcessedTuple()
                                                             << " < " << expectedResult);
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout");
        return false;
    }

    /**
     * @brief This method is used for checking if the submitted query produced the expected result within the timeout
     * @param queryId: Id of the query
     * @param expectedResult: The expected value
     * @return true if matched the expected result within the timeout
     */
    static bool checkCompleteOrTimeout(QueryId queryId, size_t expectedResult) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        size_t currentResult = 0;
        web::json::value json_return;
        std::string currentStatus = "";

        NES_DEBUG("checkCompleteOrTimeout: Check if the query goes into the Running status within the timeout");
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec && currentStatus != "RUNNING") {
            web::http::client::http_client clientProc(
                "http://localhost:8081/v1/nes/queryCatalog/status");
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

            web::http::client::http_client clientProc(
                "http://localhost:8081/v1/nes/queryCatalog/getNumberOfProducedBuffers");
            clientProc.request(web::http::methods::GET, _XPLATSTR("/"), queryId).then([](const web::http::http_response& response) {
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

            if (currentResult == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct");
                return true;
            }
            NES_DEBUG(
                "checkCompleteOrTimeout: sleep because val=" << currentResult
                                                             << " < " << expectedResult);
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout");
        return false;
    }

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NesWorker
     * @param queryId
     * @param queryCatalog
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    template<typename Predicate = std::equal_to<size_t>>
    static bool checkCompleteOrTimeout(NesWorkerPtr ptr, QueryId queryId, QueryCatalogPtr queryCatalog, size_t expectedResult) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        Predicate cmp;
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesWorkerPtr");
            auto query = queryCatalog->getQueryCatalogEntry(queryId);
            //FIXME: handle vector of statistics properly in #977
            auto statistics = ptr->getQueryStatistics(queryId);
            if (!query || statistics.empty()) {
                NES_DEBUG("checkCompleteOrTimeout: query=" << query << " stats size=" << statistics.size());
                sleep(1);
                continue;
            }
            bool isQueryRunning = query->getQueryStatus() == QueryStatus::Running;
            if (isQueryRunning && cmp(statistics[0]->getProcessedBuffers(), expectedResult)) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct isQueryRunning=" << isQueryRunning << " procBuffer=" << statistics[0]->getProcessedBuffers()
                                                                                        << " procTasks=" << statistics[0]->getProcessedTasks()
                                                                                        << " procWatermarks=" << statistics[0]->getProcessedWatermarks());
                return true;
            } else {
                NES_DEBUG("checkCompleteOrTimeout: results are incomplete isQueryRunning=" << isQueryRunning << " procBuffer=" << statistics[0]->getProcessedBuffers()
                                                                                           << " procTasks=" << statistics[0]->getProcessedTasks()
                                                                                           << " procWatermarks=" << statistics[0]->getProcessedWatermarks());
            }
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout");
        return false;
    }

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NesCoordinator
     * @param queryId
     * @param queryCatalog
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    template<typename Predicate = std::equal_to<size_t>>
    static bool checkCompleteOrTimeout(NesCoordinatorPtr ptr, QueryId queryId, QueryCatalogPtr queryCatalog, size_t expectedResult) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        Predicate cmp;
        while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesCoordinatorPtr");
            auto query = queryCatalog->getQueryCatalogEntry(queryId);
            //FIXME: handle vector of statistics properly in #977
            auto statistics = ptr->getQueryStatistics(queryId);
            if (!query || statistics.empty()) {
                continue;
            }
            bool isQueryRunning = query->getQueryStatus() == QueryStatus::Running;
            if (isQueryRunning && cmp(statistics[0]->getProcessedBuffers(), expectedResult)) {
                NES_DEBUG("checkCompleteOrTimeout: NesCoordinatorPtr results are correct isQueryRunning=" << isQueryRunning << " stats=" << statistics[0]->getProcessedBuffers()
                                                                                                          << " procTasks=" << statistics[0]->getProcessedTasks()
                                                                                                          << " procWatermarks=" << statistics[0]->getProcessedWatermarks());
                return true;
            } else {
                NES_DEBUG("checkCompleteOrTimeout: results are incomplete isQueryRunning=" << isQueryRunning << " procBuffer=" << statistics[0]->getProcessedBuffers()
                                                                                           << " procTasks=" << statistics[0]->getProcessedTasks());
            }
            sleep(1);
        }
        //FIXME: handle vector of statistics properly in #977
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout expected result=" << expectedResult
                                                                                                            << " query status=" << queryCatalog->getQueryCatalogEntry(queryId)->getQueryStatusAsString()
                                                                                                            << " processedBuffer=" << ptr->getQueryStatistics(queryId)[0]->getProcessedBuffers()
                                                                                                            << " processedTasks=" << ptr->getQueryStatistics(queryId)[0]->getProcessedTasks()
                                                                                                            << " procWatermarks=" << ptr->getQueryStatistics(queryId)[0]->getProcessedWatermarks());
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
            NES_DEBUG("checkStoppedOrTimeout: status not reached as status is=" << queryCatalog->getQueryCatalogEntry(queryId)->getQueryStatusAsString());
            sleep(1);
        }
        NES_DEBUG("checkStoppedOrTimeout: expected status not reached within set timeout");
        return false;
    }

    static TopologyNodePtr registerTestNode(size_t id, std::string address, int cpu, NodeStats nodeProperties,
                                            PhysicalStreamConfig streamConf, NodeType type, StreamCatalogPtr streamCatalog, TopologyPtr topology) {
        TopologyNodePtr nodePtr;
        if (type == NodeType::Sensor) {
            NES_DEBUG("CoordinatorService::registerNode: register sensor node");
            nodePtr = TopologyNode::create(id, address, 4000, 4002, cpu);

            NES_DEBUG("try to register sensor phyName=" << streamConf.physicalStreamName << " logName="
                                                        << streamConf.logicalStreamName << " nodeID=" << nodePtr->getId());

            //check if logical stream exists
            if (!streamCatalog->testIfLogicalStreamExistsInSchemaMapping(streamConf.logicalStreamName)) {
                NES_ERROR("Coordinator: error logical stream" << streamConf.logicalStreamName
                                                              << " does not exist when adding physical stream "
                                                              << streamConf.physicalStreamName);
                throw Exception("logical stream does not exist " + streamConf.logicalStreamName);
            }

            SchemaPtr schema = streamCatalog->getSchemaForLogicalStream(streamConf.logicalStreamName);

            DataSourcePtr source;
            if (streamConf.sourceType != "CSVSource" && streamConf.sourceType != "DefaultSource") {
                NES_ERROR("Coordinator: error source type " << streamConf.sourceType << " is not supported");
                throw Exception("Coordinator: error source type " + streamConf.sourceType
                                + " is not supported");
            }

            StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, nodePtr);

            bool success = streamCatalog->addPhysicalStream(streamConf.logicalStreamName, sce);
            if (!success) {
                NES_ERROR("Coordinator: physical stream " << streamConf.physicalStreamName
                                                          << " could not be added to catalog");
                throw Exception("Coordinator: physical stream " + streamConf.physicalStreamName
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
};
}// namespace NES
#endif//NES_INCLUDE_UTIL_TESTUTILS_HPP_
