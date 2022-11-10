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

#include <API/QueryAPI.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <iostream>

namespace NES {

class VariableLengthIntegrationTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("VariableLengthIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup VariableLengthIntegrationTest test class.");
    }
};

/// This test checks that a deployed MemorySource can write M records spanning exactly N records
TEST_F(VariableLengthIntegrationTest, testCsvSourceWithVariableLengthFields) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("VariableLengthIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("VariableLengthIntegrationTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    auto sourceCatalog = crd->getSourceCatalog();

    struct Record {
        uint64_t cameraId;
        uint64_t timestamp;
        uint64_t rows;
        uint64_t cols;
        uint64_t type;
        std::string data;
    };
    static_assert(sizeof(Record) == 72);

    auto schema = Schema::create()
                      ->addField("cameraId", DataTypeFactory::createUInt64())
                      ->addField("timestamp", DataTypeFactory::createUInt64())
                      ->addField("rows", DataTypeFactory::createUInt64())
                      ->addField("cols", DataTypeFactory::createUInt64())
                      ->addField("type", DataTypeFactory::createUInt64())
                      ->addField("data", DataTypeFactory::createText());
    ASSERT_EQ(schema->getSchemaSizeInBytes(), sizeof(Record));

    sourceCatalog->addLogicalSource("var_length_stream", schema);

    NES_INFO("VariableLengthIntegrationTest: Start worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;

    constexpr auto memAreaSize = 1 * 1024 * 1024;// 1 MB
    constexpr auto bufferSizeInNodeEngine = 4096;// TODO load this from config!
    constexpr auto buffersToExpect = memAreaSize / bufferSizeInNodeEngine;
    auto recordsToExpect = memAreaSize / schema->getSchemaSizeInBytes();
    auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));
    auto* records = reinterpret_cast<Record*>(memArea);
    size_t recordSize = schema->getSchemaSizeInBytes();
    size_t numRecords = memAreaSize / recordSize;
    for (uint64_t i = 0U; i < numRecords; ++i) {
        records[i].cameraId = i;
        records[i].timestamp = i;
        records[i].rows = i;
        records[i].cols = i;
        records[i].type = i;
        records[i].data = "1";
    }

    auto memorySourceType = MemorySourceType::create(memArea, memAreaSize, buffersToExpect, 0, "interval");
    auto physicalSource = PhysicalSource::create("var_length_stream", "stream_1", memorySourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("VariableLengthIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = "memorySourceTestOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("var_length_stream").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("VariableLengthIntegrationTest: Remove query");
    //ASSERT_TRUE(queryService->validateAndQueueStopQueryRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //    NES_INFO("VariableLengthIntegrationTest: content=" << content);
    ASSERT_TRUE(!content.empty());

    std::ifstream infile(filePath.c_str());
    std::string line;
    std::size_t lineCnt = 0;
    while (std::getline(infile, line)) {
        if (lineCnt > 0) {
            std::string expectedString = std::to_string(lineCnt - 1) + "," + std::to_string(lineCnt - 1) + ","
                + std::to_string(lineCnt - 1) + "," + std::to_string(lineCnt - 1) + "," + std::to_string(lineCnt - 1) + ","
                + std::to_string(lineCnt - 1);
            ASSERT_EQ(line, expectedString);
        }
        lineCnt++;
    }

    ASSERT_EQ(recordsToExpect, lineCnt - 1);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

}// namespace NES