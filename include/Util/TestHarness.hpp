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
#include <Operators/OperatorNode.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestUtils.hpp>
/**
 * @brief This test harness wrap query deployment test in our test framework.
 */
namespace NES {

class TestHarness {
  public:
    /*
         * @brief The constructor of TestHarness
         * @param numWorkers number of worker (each for one physical source) to be used in the test
         * @param operatorToTest operator to test
         */
    TestHarness(std::string operatorToTest) : operatorToTest(operatorToTest) {
        ipAddress = "127.0.0.1";

        NES_INFO("TestHarness: Start coordinator");
        crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
        crdPort = crd->startCoordinator(/**blocking**/ false);
        QueryServicePtr queryService = crd->getQueryService();
        QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    };

    ~TestHarness(){};

    /*
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

    /*
         * @brief add a source to be used in the test
         * @param logical stream name
         * @param schema schema of the source
         * @param physical stream name
         */
    void addSource(std::string logicalStreamName, SchemaPtr schema, std::string physicalStreamName) {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;

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
        auto wrk = std::make_shared<NesWorker>(ipAddress, crdPort, ipAddress, crdPort + (workerPtrs.size() + 1) * 20,
                                               crdPort + (workerPtrs.size() + 1) * 20 + 1, NodeType::Sensor);
        wrk->start(/**blocking**/ false, /**withConnect**/ true);
        workerPtrs.push_back(wrk);

        std::vector<uint8_t*> currentSourceRecords;
        records.push_back(currentSourceRecords);
    }

    uint64_t getWorkerCount() { return workerPtrs.size(); }

    /*
         * @brief execute the test based on the given operator, pushed elements, and number of workers,
         * then return the result of the query execution
         * @return output string
         */
    template<typename T>
    std::vector<T> getOutput(uint64_t bufferToExpect) {
        if (physicalStreamNames.size() == 0 || logicalStreamNames.size() == 0 || workerPtrs.size() == 0) {
            NES_THROW_RUNTIME_ERROR("TestHarness: source not added properly: number of added physycal streams = "
                                    + std::to_string(physicalStreamNames.size())
                                    + " number of added logical streams = " + std::to_string(logicalStreamNames.size())
                                    + " number of added workers = " + std::to_string(workerPtrs.size())
                                    + " buffers to expect = " + std::to_string(bufferToExpect));
        }

        QueryServicePtr queryService = crd->getQueryService();
        QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

        // add value collected by the record vector to the memory source
        for (int i = 0; i < sourceSchemas.size(); ++i) {
            auto currentSourceNumOfRecords = records.at(i).size();
            auto tupleSize = sourceSchemas.at(i)->getSchemaSizeInBytes();
            auto memAreaSize = currentSourceNumOfRecords * tupleSize;
            auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));

            auto currentRecords = records.at(i);
            for (int j = 0; j < currentSourceNumOfRecords; ++j) {
                memcpy(&memArea[tupleSize * j], currentRecords.at(j), tupleSize);
            }

            AbstractPhysicalStreamConfigPtr conf = MemorySourceStreamConfig::create(
                "MemorySource", physicalStreamNames.at(i), logicalStreamNames.at(i), memArea, memAreaSize);
            workerPtrs[i]->registerPhysicalStream(conf);
        }

        // local fs
        std::string filePath = "testHarness.out";
        remove(filePath.c_str());

        //register query
        std::string queryString =
            operatorToTest + R"(.sink(FileSinkDescriptor::create(")" + filePath + R"(" , "NES_FORMAT", "APPEND"));)";
        QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");

        auto globalQueryPlan = crd->getGlobalQueryPlan();
        if (!TestUtils::waitForQueryToStart(queryId, queryCatalog)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: waitForQueryToStart returns false");
        }

        if (!TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, bufferToExpect)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: checkCompleteOrTimeout returns false: queryId=" + std::to_string(queryId)
                                    + " number of buffers to expect=" + std::to_string(bufferToExpect));
        }

        NES_INFO("QueryDeploymentTest: Remove query");
        if (!queryService->validateAndQueueStopRequest(queryId)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: cannot validateAndQueueStopRequest for query with id=" + queryId);
        }
        if (!TestUtils::checkStoppedOrTimeout(queryId, queryCatalog)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: checkStoppedOrTimeout returns false for query with id= " + queryId);
        }

        std::ifstream ifs(filePath.c_str());
        if (!ifs.good()) {
            NES_WARNING("TestHarness:ifs.good() returns false for query with id " + queryId << " file path=" + filePath);
        }

        // check the length of the output file
        ifs.seekg (0, ifs.end);
        auto length = ifs.tellg();
        ifs.seekg (0, ifs.beg);

        // read the binary output as a vector of T
        auto* buff = reinterpret_cast<char*>(malloc(length));
        ifs.read(buff, length);
        std::vector<T> actualOutput(reinterpret_cast<T*>(buff), reinterpret_cast<T*>(buff)+ length/sizeof(T));

        for (NesWorkerPtr wrk : workerPtrs) {
            wrk->stop(false);
        }
        crd->stopCoordinator(false);

        return actualOutput;
    }

  private:
    NesCoordinatorPtr crd;
    uint64_t crdPort;
    std::string ipAddress;

    std::string operatorToTest;

    std::vector<NesWorkerPtr> workerPtrs;
    std::vector<std::vector<uint8_t*>> records;
    std::vector<uint64_t> counters;
    std::vector<SchemaPtr> sourceSchemas;
    std::vector<std::string> physicalStreamNames;
    std::vector<std::string> logicalStreamNames;

    uint64_t restPort = 8081;
    uint64_t rpcPort = 4000;
};

typedef std::shared_ptr<TestHarness> TestHarnessPtr;
}// namespace NES

#endif//NES_TESTHARNESS_HPP
