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

#ifndef NES_TESTHARNESS_HPP
#define NES_TESTHARNESS_HPP
#include <API/Schema.hpp>
#include <Catalogs/MemorySourceStreamConfig.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestUtils.hpp>

/**
 * @brief This test harness wrap query deployment test in our test framework.
 */
namespace NES {

class TestHarness {
  public:
    /**
         * @brief The constructor of TestHarness
         * @param numWorkers number of worker (each for one physical source) to be used in the test
         * @param queryWithoutSink query string to test (without the sink operator)
         * @param restPort port for the rest service
         * @param rpcPort for for the grpc
         */
    TestHarness(std::string queryWithoutSink, uint16_t restPort = 8081, uint16_t rpcPort = 4000)
        : ipAddress("127.0.0.1"), queryWithoutSink(queryWithoutSink), bufferSize(4096) {
        NES_INFO("TestHarness: Start coordinator");
        crdConf = CoordinatorConfig::create();
        crdConf->resetCoordinatorOptions();
        crdConf->setCoordinatorIp(ipAddress);
        crdConf->setRestPort(restPort);
        crdConf->setRpcPort(rpcPort);
        wrkConf = WorkerConfig::create();
        crd = std::make_shared<NesCoordinator>(crdConf);
        crdPort = crd->startCoordinator(/**blocking**/ false);
    };

    ~TestHarness() { NES_DEBUG("TestHarness: ~TestHarness()"); };

    /**
         * @brief push a single element/tuple to specific source
         * @param element element of Record to push
         * @param sourceIdx index of the source to which the element is pushed
         */
    template<typename T>
    void pushElement(T element, uint64_t sourceIdx) {
        if (sourceIdx >= sourceSchemas.size()) {
            NES_THROW_RUNTIME_ERROR("TestHarness: sourceIdx is out of bound");
        }

        if (!std::is_class<T>::value) {
            NES_THROW_RUNTIME_ERROR("TestHarness: tuples must be instances of struct");
        }

        if (sizeof(T) != sourceSchemas.at(sourceIdx)->getSchemaSizeInBytes()) {
            NES_THROW_RUNTIME_ERROR("TestHarness: tuple size and schema size does not match");
        }

        auto* memArea = reinterpret_cast<uint8_t*>(malloc(sizeof(T)));
        memcpy(memArea, reinterpret_cast<uint8_t*>(&element), sizeof(T));
        records[sourceIdx].push_back(memArea);
    }

    /**
         * @brief check the schema size of the logical stream and if it already exists
         * @param logical stream name
         * @param schema schema of the source
         * @param physical stream name
         */
    void checkAndAddSource(std::string logicalStreamName, SchemaPtr schema, std::string physicalStreamName, uint64_t parentId) {
        // Check if logical stream already exists
        if (!crd->getStreamCatalog()->testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)) {
            NES_TRACE("TestHarness: logical source does not exist in the stream catalog, adding a new logical stream "
                      << logicalStreamName);
            crd->getStreamCatalog()->addLogicalStream(logicalStreamName, schema);
        } else {
            // Check if it has the same schema
            if (!crd->getStreamCatalog()->getSchemaForLogicalStream(logicalStreamName)->equals(schema, true)) {
                NES_TRACE("TestHarness: logical source " << logicalStreamName
                                                         << " exists in the stream catalog with "
                                                            "different schema, replacing it with a new schema");
                crd->getStreamCatalog()->removeLogicalStream(logicalStreamName);
                crd->getStreamCatalog()->addLogicalStream(logicalStreamName, schema);
            }
        }

        physicalStreamNames.push_back(physicalStreamName);
        logicalStreamNames.push_back(logicalStreamName);
        sourceSchemas.push_back(schema);

        // set the localWorkerRpcPort and localWorkerZmqPort based on the number of workers
        wrkConf->resetWorkerOptions();
        wrkConf->setCoordinatorPort(crdPort);
        wrkConf->setRpcPort(crdPort + (workerPtrs.size() + 1) * 20);
        wrkConf->setDataPort(crdPort + (workerPtrs.size() + 1) * 20 + 1);
        auto wrk = std::make_shared<NesWorker>(wrkConf, NodeType::Sensor);
        wrk->start(/**blocking**/ false, /**withConnect**/ true);
        wrk->replaceParent(crd->getTopology()->getRoot()->getId(), parentId);
        workerPtrs.push_back(wrk);
    }

    /**
         * @brief add a memory source to be used in the test and connect to parent with specific parent id
         * @param logical stream name
         * @param schema schema of the source
         * @param physical stream name
         * @param parentId id of the parent to connect
         */
    void addMemorySource(std::string logicalStreamName, SchemaPtr schema, std::string physicalStreamName, uint64_t parentId) {
        NES_ASSERT(parentId != INVALID_TOPOLOGY_NODE_ID, "The provided ParentId is an INVALID_TOPOLOGY_NODE_ID");
        // check if record may span multiple buffers
        NES_ASSERT2_FMT(bufferSize % schema->getSchemaSizeInBytes() == 0,
                        "TestHarness: A record might span multiple buffers and this is not supported bufferSize="
                            << bufferSize << " recordSize=" << schema->getSchemaSizeInBytes());
        checkAndAddSource(logicalStreamName, schema, physicalStreamName, parentId);

        sourceTypes.push_back(MemorySource);
        std::vector<uint8_t*> currentSourceRecords;
        records.push_back(currentSourceRecords);
    }

    /**
         * @brief add a memory source to be used in the test
         * @param logical stream name
         * @param schema schema of the source
         * @param physical stream name
         */
    void addMemorySource(std::string logicalStreamName, SchemaPtr schema, std::string physicalStreamName) {
        uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
        addMemorySource(logicalStreamName, schema, physicalStreamName, crdTopologyNodeId);
    }

    /**
         * @brief add a csv source to be used in the test and connect to parent with specific parent id
         * @param schema schema of the source
         * @param csvSourceConf physical stream configuration for the csv source
         * @param parentId id of the parent to connect
         */
    void addCSVSource(PhysicalStreamConfigPtr csvSourceConf, SchemaPtr schema, uint64_t parentId) {
        checkAndAddSource(csvSourceConf->getLogicalStreamName(), schema, csvSourceConf->getPhysicalStreamName(), parentId);

        csvSourceConfs.push_back(csvSourceConf);
        sourceTypes.push_back(CSVSource);

        std::vector<uint8_t*> currentSourceRecords;
        records.push_back(currentSourceRecords);
    }

    /**
      * @brief add a csv source to be used in the test
      * @param schema schema of the source
      * @param csvSourceConf physical stream configuration for the csv source
      */
    void addCSVSource(PhysicalStreamConfigPtr csvSourceConf, SchemaPtr schema) {
        uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
        addCSVSource(csvSourceConf, schema, crdTopologyNodeId);
    }

    /**
     * @brief add non source worker and connect to parent with specific parent id
     * @param parentId id of the parent to connect
     */
    void addNonSourceWorker(uint64_t parentId) {
        wrkConf->resetWorkerOptions();
        wrkConf->setCoordinatorPort(crdPort);
        wrkConf->setRpcPort(crdPort + (workerPtrs.size() + 1) * 20);
        wrkConf->setDataPort(crdPort + (workerPtrs.size() + 1) * 20 + 1);
        auto wrk = std::make_shared<NesWorker>(wrkConf, NodeType::Sensor);
        wrk->start(/**blocking**/ false, /**withConnect**/ true);
        wrk->replaceParent(crd->getTopology()->getRoot()->getId(), parentId);
        workerPtrs.push_back(wrk);
    }

    /**
         * @brief add non source worker
         */
    void addNonSourceWorker() {
        uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();
        addNonSourceWorker(crdTopologyNodeId);
    }

    uint64_t getWorkerCount() { return workerPtrs.size(); }

    /**
         * @brief execute the test based on the given operator, pushed elements, and number of workers,
         * then return the result of the query execution
         * @return output string
         */
    template<typename T>
    std::vector<T> getOutput(uint64_t numberOfContentToExpect) {
        if (physicalStreamNames.size() == 0 || logicalStreamNames.size() == 0 || workerPtrs.size() == 0) {
            NES_THROW_RUNTIME_ERROR("TestHarness: source not added properly: number of added physycal streams = "
                                    << std::to_string(physicalStreamNames.size())
                                    << " number of added logical streams = " << std::to_string(logicalStreamNames.size())
                                    << " number of added workers = " << std::to_string(workerPtrs.size())
                                    << " number of content to expect = " << std::to_string(numberOfContentToExpect));
        }

        QueryServicePtr queryService = crd->getQueryService();
        QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

        // add value collected by the record vector to the memory source
        for (int i = 0; i < sourceTypes.size(); ++i) {
            if (sourceTypes[i] == CSVSource) {
                // use the given csv source
                workerPtrs[i]->registerPhysicalStream(csvSourceConfs[i]);
            } else if (sourceTypes[i] == MemorySource) {
                // create and populate memory source
                auto currentSourceNumOfRecords = records.at(i).size();
                auto tupleSize = sourceSchemas.at(i)->getSchemaSizeInBytes();
                auto memAreaSize = currentSourceNumOfRecords * tupleSize;
                auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));

                auto currentRecords = records.at(i);
                for (int j = 0; j < currentSourceNumOfRecords; ++j) {
                    memcpy(&memArea[tupleSize * j], currentRecords.at(j), tupleSize);
                }

                //TODO: we have to fix those hard values
                AbstractPhysicalStreamConfigPtr conf = MemorySourceStreamConfig::create(
                    "MemorySource", physicalStreamNames.at(i), logicalStreamNames.at(i), memArea, memAreaSize, 1, 0);
                workerPtrs[i]->registerPhysicalStream(conf);
            } else {
                NES_THROW_RUNTIME_ERROR("TestHarness:getOutput: Unknown source type:" << std::to_string(sourceTypes[i]));
            }
        }

        // local fs
        std::string filePath = "testHarness.out";
        remove(filePath.c_str());

        //register query
        std::string queryString =
            queryWithoutSink + R"(.sink(FileSinkDescriptor::create(")" + filePath + R"(" , "NES_FORMAT", "APPEND"));)";
        QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");

        if (!TestUtils::waitForQueryToStart(queryId, queryCatalog)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: waitForQueryToStart returns false");
        }

        // Check if the size of output struct match with the size of output schema
        // Output struct might be padded, in this case the size is not equal to the total size of its field
        // Currently, we need to produce a result with the schema that does not cause the associated struct to be padded
        // (e.g., the size is multiple of 8)
        uint64_t outputSchemaSizeInBytes = queryCatalog->getQueryCatalogEntry(queryId)
                                               ->getQueryPlan()
                                               ->getSinkOperators()[0]
                                               ->getOutputSchema()
                                               ->getSchemaSizeInBytes();
        NES_ASSERT(outputSchemaSizeInBytes == sizeof(T),
                   "The size of output struct does not match output schema."
                   " Output struct:"
                       << std::to_string(sizeof(T)) << " Schema:" << std::to_string(outputSchemaSizeInBytes));

        if (!TestUtils::checkBinaryOutputContentLengthOrTimeout<T>(numberOfContentToExpect, filePath)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: checkBinaryOutputContentLengthOrTimeout returns false "
                                    "number of buffers to expect="
                                    << std::to_string(numberOfContentToExpect));
        }

        NES_INFO("QueryDeploymentTest: Remove query");
        if (!queryService->validateAndQueueStopRequest(queryId)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: cannot validateAndQueueStopRequest for query with id=" << queryId);
        }
        if (!TestUtils::checkStoppedOrTimeout(queryId, queryCatalog)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: checkStoppedOrTimeout returns false for query with id= " << queryId);
        }

        std::ifstream ifs(filePath.c_str());
        if (!ifs.good()) {
            NES_WARNING("TestHarness:ifs.good() returns false for query with id " << queryId << " file path=" << filePath);
        }

        // check the length of the output file
        ifs.seekg(0, ifs.end);
        auto length = ifs.tellg();
        ifs.seekg(0, ifs.beg);

        // read the binary output as a vector of T
        auto* buff = reinterpret_cast<char*>(malloc(length));
        ifs.read(buff, length);
        std::vector<T> actualOutput(reinterpret_cast<T*>(buff), reinterpret_cast<T*>(buff) + length / sizeof(T));

        for (NesWorkerPtr wrk : workerPtrs) {
            wrk->stop(false);
        }
        crd->stopCoordinator(false);

        return actualOutput;
    }

    TopologyPtr getTopology() { return crd->getTopology(); };

    /*
     * @brief get the id of worker at a particular index
     * @param workerIdx index of the worker in the test harness
     */
    uint64_t getWorkerId(uint64_t workerIdx) { return workerPtrs.at(workerIdx)->getTopologyNodeId(); }

  private:
    enum TestHarnessSourceType { CSVSource, MemorySource };
    CoordinatorConfigPtr crdConf;
    WorkerConfigPtr wrkConf;
    NesCoordinatorPtr crd;
    uint64_t crdPort;
    std::string ipAddress;

    std::string queryWithoutSink;

    std::vector<NesWorkerPtr> workerPtrs;
    std::vector<std::vector<uint8_t*>> records;
    std::vector<uint64_t> counters;
    std::vector<SchemaPtr> sourceSchemas;
    std::vector<std::string> physicalStreamNames;
    std::vector<std::string> logicalStreamNames;
    std::vector<PhysicalStreamConfigPtr> csvSourceConfs;
    std::vector<TestHarnessSourceType> sourceTypes;
    uint64_t bufferSize;
};
}// namespace NES

#endif//NES_TESTHARNESS_HPP
