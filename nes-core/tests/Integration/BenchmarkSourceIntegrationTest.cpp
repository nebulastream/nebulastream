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
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/BenchmarkSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include "../util/NesBaseTest.hpp"
#include <iostream>

namespace NES {

class BenchmarkSourceIntegrationTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("BenchmarkSourceIntegrationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup BenchmarkSourceIntegrationTest test class.");
    }

};

/// This test checks that a deployed BenchmarkSource can write M records spanning exactly N records
TEST_F(BenchmarkSourceIntegrationTest, testBenchmarkSource) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("BenchmarkSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("BenchmarkSourceIntegrationTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    auto streamCatalog = crd->getStreamCatalog();

    struct Record {
        uint64_t key;
        uint64_t timestamp;
    };
    static_assert(sizeof(Record) == 16);

    auto schema = Schema::create()
                      ->addField("key", DataTypeFactory::createUInt64())
                      ->addField("timestamp", DataTypeFactory::createUInt64());
    ASSERT_EQ(schema->getSchemaSizeInBytes(), sizeof(Record));

    streamCatalog->addLogicalStream("memory_stream", schema);

    NES_INFO("BenchmarkSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    constexpr auto memAreaSize = 4096;           // 1 MB
    constexpr auto bufferSizeInNodeEngine = 4096;// TODO load this from config!
    constexpr auto buffersToExpect = memAreaSize / bufferSizeInNodeEngine;
    auto recordsToExpect = memAreaSize / schema->getSchemaSizeInBytes();
    auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));
    auto* records = reinterpret_cast<Record*>(memArea);
    size_t recordSize = schema->getSchemaSizeInBytes();
    size_t numRecords = memAreaSize / recordSize;
    for (auto i = 0U; i < numRecords; ++i) {
        records[i].key = i;
        records[i].timestamp = i;
    }

    auto benchmarkSourceType =
        BenchmarkSourceType::create(memArea, memAreaSize, buffersToExpect, 0, "frequency", "copyBuffer", 0, 0);
    auto physicalSource = PhysicalSource::create("memory_stream", "memory_stream_0", benchmarkSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("BenchmarkSourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = "benchmSourceTestOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("memory_stream").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("BenchmarkSourceIntegrationTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //    NES_INFO("BenchmarkSourceIntegrationTest: content=" << content);
    EXPECT_TRUE(!content.empty());

    std::ifstream infile(filePath.c_str());
    std::string line;
    std::size_t lineCnt = 0;
    while (std::getline(infile, line)) {
        if (lineCnt > 0) {
            std::string expectedString = std::to_string(lineCnt - 1) + "," + std::to_string(lineCnt - 1);
            ASSERT_EQ(line, expectedString);
        }
        lineCnt++;
    }

    ASSERT_EQ(recordsToExpect, lineCnt - 1);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/// This test checks that a deployed MemorySource can write M records stored in one buffer that is not full
TEST_F(BenchmarkSourceIntegrationTest, testMemorySourceFewTuples) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    NES_INFO("BenchmarkSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("BenchmarkSourceIntegrationTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    auto streamCatalog = crd->getStreamCatalog();

    struct Record {
        uint64_t key;
        uint64_t timestamp;
    };
    static_assert(sizeof(Record) == 16);

    auto schema = Schema::create()
                      ->addField("key", DataTypeFactory::createUInt64())
                      ->addField("timestamp", DataTypeFactory::createUInt64());
    ASSERT_EQ(schema->getSchemaSizeInBytes(), sizeof(Record));

    streamCatalog->addLogicalStream("memory_stream", schema);

    NES_INFO("BenchmarkSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    constexpr auto memAreaSize = sizeof(Record) * 5;
    constexpr auto bufferSizeInNodeEngine = 4096;// TODO load this from config!
    constexpr auto buffersToExpect = memAreaSize / bufferSizeInNodeEngine;
    auto recordsToExpect = memAreaSize / schema->getSchemaSizeInBytes();
    auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));
    auto* records = reinterpret_cast<Record*>(memArea);
    size_t recordSize = schema->getSchemaSizeInBytes();
    size_t numRecords = memAreaSize / recordSize;
    for (auto i = 0U; i < numRecords; ++i) {
        records[i].key = i;
        records[i].timestamp = i;
    }

    auto benchmarkSourceType = BenchmarkSourceType::create(memArea, memAreaSize, 1, 0, "frequency", "copyBuffer", 0, 0);
    auto physicalSource = PhysicalSource::create("memory_stream", "memory_stream_0", benchmarkSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("BenchmarkSourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = "benchmSourceTestOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("memory_stream").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("BenchmarkSourceIntegrationTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("BenchmarkSourceIntegrationTest: content=" << content);
    EXPECT_TRUE(!content.empty());

    std::ifstream infile(filePath.c_str());
    std::string line;
    std::size_t lineCnt = 0;
    while (std::getline(infile, line)) {
        if (lineCnt > 0) {
            std::string expectedString = std::to_string(lineCnt - 1) + "," + std::to_string(lineCnt - 1);
            ASSERT_EQ(line, expectedString);
        }
        lineCnt++;
    }

    ASSERT_EQ(recordsToExpect, lineCnt - 1);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/// This test checks that a deployed MemorySource can write M records stored in N+1 buffers
/// with the invariant that the N+1-th buffer is half full

TEST_F(BenchmarkSourceIntegrationTest, DISABLED_testMemorySourceHalfFullBuffer) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    NES_INFO("BenchmarkSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("BenchmarkSourceIntegrationTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    auto streamCatalog = crd->getStreamCatalog();

    struct Record {
        uint64_t key;
        uint64_t timestamp;
    };
    static_assert(sizeof(Record) == 16);

    auto schema = Schema::create()
                      ->addField("key", DataTypeFactory::createUInt64())
                      ->addField("timestamp", DataTypeFactory::createUInt64());
    ASSERT_EQ(schema->getSchemaSizeInBytes(), sizeof(Record));

    streamCatalog->addLogicalStream("memory_stream", schema);

    NES_INFO("BenchmarkSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    constexpr auto bufferSizeInNodeEngine = 4096;// TODO load this from config!
    constexpr auto memAreaSize = bufferSizeInNodeEngine * 64 + (bufferSizeInNodeEngine / 2);
    constexpr auto buffersToExpect = memAreaSize / bufferSizeInNodeEngine;
    auto recordsToExpect = memAreaSize / schema->getSchemaSizeInBytes();
    auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));
    auto* records = reinterpret_cast<Record*>(memArea);
    size_t recordSize = schema->getSchemaSizeInBytes();
    size_t numRecords = memAreaSize / recordSize;
    for (auto i = 0U; i < numRecords; ++i) {
        records[i].key = i;
        records[i].timestamp = i;
    }

    auto benchmarkSourceType =
        BenchmarkSourceType::create(memArea, memAreaSize, buffersToExpect + 1, 0, "frequency", "copyBuffer", 0, 0);
    auto physicalSource = PhysicalSource::create("memory_stream", "memory_stream_0", benchmarkSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("BenchmarkSourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = "benchmSourceTestOut";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("memory_stream").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("BenchmarkSourceIntegrationTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("BenchmarkSourceIntegrationTest: content=" << content);
    EXPECT_TRUE(!content.empty());

    std::ifstream infile(filePath.c_str());
    std::string line;
    std::size_t lineCnt = 0;
    while (std::getline(infile, line)) {
        if (lineCnt > 0) {
            std::string expectedString = std::to_string(lineCnt - 1) + "," + std::to_string(lineCnt - 1);
            std::cout << " line=" << line << " expected=" << expectedString << std::endl;
            ASSERT_EQ(line, expectedString);
        }
        lineCnt++;
    }

    ASSERT_EQ(recordsToExpect, lineCnt - 1);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES