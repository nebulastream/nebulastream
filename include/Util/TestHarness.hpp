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
#include <Operators/OperatorNode.hpp>
#include <Util/TestUtils.hpp>
#include <Catalogs/MemorySourceStreamConfig.hpp>
#include <Services/QueryService.hpp>
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

        if (sizeof(T) != sourceSchemas.at(sourceIdx)->getSchemaSizeInBytes()){
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

        if (!crd->getStreamCatalog()->testIfLogicalStreamExistsInSchemaMapping(logicalStreamName)){
            crd->getStreamCatalog()->addLogicalStream(logicalStreamName, schema);
        }


        workerCount++;
        auto wrk = std::make_shared<NesWorker>(ipAddress, crdPort, ipAddress,
                                               crdPort + (workerCount)*20, crdPort + (workerCount)*20+1,
                                               NodeType::Sensor);
        workerPtrs.push_back(wrk);
        wrk->start(/**blocking**/ false, /**withConnect**/ true);

        physicalStreamNames.push_back(physicalStreamName);
        logicalStreamNames.push_back(logicalStreamName);
        sourceSchemas.push_back(schema);

        std::vector<uint8_t*> currentSourceRecords;
        records.push_back(currentSourceRecords);
    }

    uint64_t getWorkerCount() {
        return workerPtrs.size();
    }

    /*
         * @brief execute the test based on the given operator, pushed elements, and number of workers,
         * then return the result of the query execution
         * @return output string
         */
    std::string getOutput(uint64_t bufferToExpect) {
        if (physicalStreamNames.size() == 0 || logicalStreamNames.size() == 0 || workerPtrs.size() == 0) {
            NES_THROW_RUNTIME_ERROR("TestHarness: source not added properly");
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
            for (int j=0; j<currentSourceNumOfRecords; ++j){
                memcpy(&memArea[tupleSize*j], currentRecords.at(j), tupleSize);
            }

            AbstractPhysicalStreamConfigPtr conf = MemorySourceStreamConfig::create(
                "MemorySource", physicalStreamNames.at(i), logicalStreamNames.at(i), memArea, memAreaSize);
            workerPtrs[i]->registerPhysicalStream(conf);
        }

        // local fs
        std::string filePath = "contTestOut.csv";
        remove(filePath.c_str());

        //register query
        // TODO: maybe move this Query::from to test driver, as tester may want to use more than 1 source
        std::string queryString = operatorToTest + R"(.sink(FileSinkDescriptor::create(")" + filePath
            + R"(" , "CSV_FORMAT", "APPEND"));)";
        QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");

        auto globalQueryPlan = crd->getGlobalQueryPlan();
        if (!TestUtils::waitForQueryToStart(queryId, queryCatalog)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: waitForQueryToStart returns false");
        }

        if (!TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, bufferToExpect)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: checkCompleteOrTimeout returns false");
        }

        NES_INFO("QueryDeploymentTest: Remove query");
        if (!queryService->validateAndQueueStopRequest(queryId)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: cannot validateAndQueueStopRequest");
        }
        if (!TestUtils::checkStoppedOrTimeout(queryId, queryCatalog)) {
            NES_THROW_RUNTIME_ERROR("TestHarness: checkStoppedOrTimeout returns false");
        }

        std::ifstream ifs(filePath.c_str());
        if (!ifs.good()) {
            NES_WARNING("TestHarness:ifs.good() returns false");
        }

        std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

        for (NesWorkerPtr wrk : workerPtrs) {
            wrk->stop(false);
        }
        crd->stopCoordinator(false);

        return content;
    }


  private:
    NesCoordinatorPtr crd;
    uint64_t crdPort;
    uint64_t workerCount;
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
