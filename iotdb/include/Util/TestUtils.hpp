#ifndef NES_INCLUDE_UTIL_TESTUTILS_HPP_
#define NES_INCLUDE_UTIL_TESTUTILS_HPP_
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <chrono>
#include <iostream>
#include <memory>
using Seconds = std::chrono::seconds;
using Clock = std::chrono::high_resolution_clock;
using namespace std;

namespace NES {

/**
 * @brief this is a util class for the tests
 */
class TestUtils {
  public:
    static const size_t timeout = 10;
    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NodeEngine
     * @param queryId
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    static bool checkCompleteOrTimeout(NodeEnginePtr ptr, std::string queryId, size_t expectedResult) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        auto now = start_timestamp;
        while ((now = std::chrono::system_clock::now()) < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NodeEnginePtr");
            if (ptr->getQueryStatistics(queryId)->getProcessedBuffers() == expectedResult
                && ptr->getQueryStatistics(queryId)->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct");
                return true;
            }
            NES_DEBUG(
                "checkCompleteOrTimeout: sleep because val=" << ptr->getQueryStatistics(queryId)->getProcessedTuple()
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
         * @param expectedResult
         * @return bool indicating if the expected results are matched
         */
    static bool checkCompleteOrTimeout(NesWorkerPtr ptr, std::string queryId, size_t expectedResult) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        auto now = start_timestamp;
        while ((now = std::chrono::system_clock::now()) < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesWorkerPtr");
            if (ptr->getQueryStatistics(queryId)->getProcessedBuffers() == expectedResult
                && ptr->getQueryStatistics(queryId)->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct");
                return true;
            }
            NES_DEBUG(
                "checkCompleteOrTimeout: sleep because val=" << ptr->getQueryStatistics(queryId)->getProcessedTuple()
                                                             << " < " << expectedResult);
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout");
        return false;
    }

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NesCoordinator
     * @param queryId
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    static bool checkCompleteOrTimeout(NesCoordinatorPtr ptr, std::string queryId, size_t expectedResult) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        auto now = start_timestamp;
        while ((now = std::chrono::system_clock::now()) < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesCoordinatorPtr");
            if (ptr->getQueryStatistics(queryId)->getProcessedBuffers() == expectedResult
                && ptr->getQueryStatistics(queryId)->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct");
                return true;
            }
            NES_DEBUG(
                "checkCompleteOrTimeout: sleep because val=" << ptr->getQueryStatistics(queryId)->getProcessedTuple()
                                                             << " < " << expectedResult);
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout");
        return false;
    }

    static NESTopologyEntryPtr registerTestNode(size_t id, std::string address, int cpu, const string& nodeProperties,
                                                PhysicalStreamConfig streamConf, NESNodeType type) {
        NESTopologyEntryPtr nodePtr;
        if (type == NESNodeType::Sensor) {
            NES_DEBUG("CoordinatorService::registerNode: register sensor node");
            nodePtr = NESTopologyManager::getInstance().createNESSensorNode(id, address, CPUCapacity::Value(cpu));
            NESTopologySensorNode* sensor = dynamic_cast<NESTopologySensorNode*>(nodePtr.get());
            sensor->setPhysicalStreamName(streamConf.physicalStreamName);

            NES_DEBUG(
                "try to register sensor phyName=" << streamConf.physicalStreamName << " logName="
                                                  << streamConf.logicalStreamName << " nodeID=" << nodePtr->getId());

            //check if logical stream exists
            if (!StreamCatalog::instance().testIfLogicalStreamExistsInSchemaMapping(
                    streamConf.logicalStreamName)) {
                NES_ERROR(
                    "Coordinator: error logical stream" << streamConf.logicalStreamName
                                                        << " does not exist when adding physical stream "
                                                        << streamConf.physicalStreamName);
                throw Exception(
                    "logical stream does not exist " + streamConf.logicalStreamName);
            }

            SchemaPtr schema = StreamCatalog::instance().getSchemaForLogicalStream(
                streamConf.logicalStreamName);

            DataSourcePtr source;
            if (streamConf.sourceType != "CSVSource"
                && streamConf.sourceType != "DefaultSource") {
                NES_ERROR(
                    "Coordinator: error source type " << streamConf.sourceType << " is not supported");
                throw Exception(
                    "Coordinator: error source type " + streamConf.sourceType
                    + " is not supported");
            }

            StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
                streamConf, nodePtr);

            bool success = StreamCatalog::instance().addPhysicalStream(
                streamConf.logicalStreamName, sce);
            if (!success) {
                NES_ERROR(
                    "Coordinator: physical stream " << streamConf.physicalStreamName
                                                    << " could not be added to catalog");
                throw Exception(
                    "Coordinator: physical stream " + streamConf.physicalStreamName
                    + " could not be added to catalog");
            }

        } else if (type == NESNodeType::Worker) {
            NES_DEBUG("CoordinatorService::registerNode: register worker node");
            nodePtr = NESTopologyManager::getInstance().createNESWorkerNode(id, address, CPUCapacity::Value(cpu));
        } else {
            NES_ERROR("CoordinatorService::registerNode: type not supported " << type);
            assert(0);
        }
        assert(nodePtr);

        if (nodeProperties != "defaultProperties") {
            nodePtr->setNodeProperty(nodeProperties);
        }

        const NESTopologyEntryPtr kRootNode = NESTopologyManager::getInstance()
                                                  .getRootNode();

        if (kRootNode == nodePtr) {
            NES_DEBUG("CoordinatorService::registerNode: tree is empty so this becomes new root");
        } else {
            NES_DEBUG("CoordinatorService::registerNode: add link to root node " << kRootNode << " of type"
                                                                                 << kRootNode->getEntryType());
            NESTopologyManager::getInstance().createNESTopologyLink(nodePtr, kRootNode, 1, 1);
        }

        return nodePtr;
    }
};
}// namespace NES
#endif//NES_INCLUDE_UTIL_TESTUTILS_HPP_
