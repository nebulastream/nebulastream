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

#include <Catalogs/MemorySourceStreamConfig.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestHarness.hpp>
#include <Util/TestUtils.hpp>

namespace NES {

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

TestHarness::TestHarness(uint64_t numWorkers,
                         std::string operatorToTest) : numWorkers(numWorkers),
                                                       operatorToTest(operatorToTest) {
    std::string ipAddress = "127.0.0.1";
    rpcPort = rpcPort + 30;
    restPort = restPort + 2;

    NES_INFO("TestHarness: Start coordinator");
    crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    StreamCatalogPtr streamCatalog = crd->getStreamCatalog();

    schema = Schema::create()
        ->addField("key", DataTypeFactory::createUInt32())
        ->addField("value", DataTypeFactory::createUInt32())
        ->addField("timestamp", DataTypeFactory::createUInt64());

    streamCatalog->addLogicalStream("memory_stream", schema);
    // initiate sources based on the number of worker
    // currently support only a single logical source

    for (int i=0; i<numWorkers; i++) {
        auto wrk = std::make_shared<NesWorker>(ipAddress, port, ipAddress,
                                               port + (i+1)*20, port + (i+1)*20+1,
                                               NodeType::Sensor);
        workerPtrs.push_back(wrk);
        wrk->start(/**blocking**/ false, /**withConnect**/ true);

        std::vector<Record> currentSourceRecords;
        records.push_back(currentSourceRecords);
    }
}
void TestHarness::pushElement(Record element, uint64_t sourceIdx) {
    // push an element to a specific source
    NES_INFO("TestHarness: pushed element (" << std::to_string(element.key) <<"," << std::to_string(element.value) << ","
                                              << std::to_string(element.timestamp) << ") to source=" << std::to_string(sourceIdx));
    records[sourceIdx].push_back(element);
}
std::string TestHarness::getOutput(uint64_t bufferToExpect) {
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    // add value collected by the record vector to the memory source
    for(int i = 0; i < numWorkers; ++i) {
        auto currentSourceNumOfRecords = records.at(i).size();
        // TODO: adjust based on the number of records
        auto memAreaSize = 4096;
        auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));

        Record* currentSourceRecord = reinterpret_cast<Record*>(memArea);
        for (auto j = 0u; j < currentSourceNumOfRecords; ++j) {
            currentSourceRecord[j].key = records.at(i).at(j).key;
            currentSourceRecord[j].value = records.at(i).at(j).value;
            currentSourceRecord[j].timestamp = records.at(i).at(j).timestamp;
        }

        AbstractPhysicalStreamConfigPtr conf =
            MemorySourceStreamConfig::create("MemorySource", "memory_stream_"+std::to_string(i), "memory_stream", memArea, memAreaSize);
        workerPtrs[i]->registerPhysicalStream(conf);
    }

    // local fs
    std::string filePath = "contTestOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("memory_stream"))" + operatorToTest + R"(.sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");

    auto globalQueryPlan = crd->getGlobalQueryPlan();
    if (!TestUtils::waitForQueryToStart(queryId, queryCatalog)){
        NES_THROW_RUNTIME_ERROR("TestHarness: waitForQueryToStart returns false");
    }

    if (!TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, bufferToExpect)){
        NES_THROW_RUNTIME_ERROR("TestHarness: checkCompleteOrTimeout returns false");
    }

    NES_INFO("QueryDeploymentTest: Remove query");
    if (!queryService->validateAndQueueStopRequest(queryId)){
        NES_THROW_RUNTIME_ERROR("TestHarness: cannot validateAndQueueStopRequest");
    }
    if (!TestUtils::checkStoppedOrTimeout(queryId, queryCatalog)){
        NES_THROW_RUNTIME_ERROR("TestHarness: checkStoppedOrTimeout returns false");
    }

    std::ifstream ifs(filePath.c_str());
    if (!ifs.good()){
        NES_WARNING("TestHarness:ifs.good() returns false");
    }

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    for(NesWorkerPtr wrk: workerPtrs) {
        wrk->stop(false);
    }
    crd->stopCoordinator(false);

    return content;
}
} // namespace NES