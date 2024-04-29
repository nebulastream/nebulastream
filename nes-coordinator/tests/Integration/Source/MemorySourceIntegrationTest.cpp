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
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <iostream>

namespace NES {

class MemorySourceIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MemorySourceIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MemorySourceIntegrationTest test class.");
    }
};

/// This test checks that a deployed MemorySource can write M records spanning exactly N records
TEST_F(MemorySourceIntegrationTest, testMemorySource) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MemorySourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MemorySourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    struct Record {
        uint64_t key;
        uint64_t timestamp;
    };
    static_assert(sizeof(Record) == 16);

    auto schema = Schema::create()
                      ->addField("key", DataTypeFactory::createUInt64())
                      ->addField("timestamp", DataTypeFactory::createUInt64());
    ASSERT_EQ(schema->getSchemaSizeInBytes(), sizeof(Record));

    sourceCatalog->addLogicalSource("memory_stream", schema);

    NES_INFO("MemorySourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    wrkConf->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    constexpr auto memAreaSize = 1 * 1024 * 1024;// 1 MB
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

    auto memorySourceType = MemorySourceType::create("memory_stream",
                                                     "memory_stream_0",
                                                     memArea,
                                                     memAreaSize,
                                                     buffersToExpect,
                                                     0,
                                                     GatheringMode::INTERVAL_MODE);

    wrkConf->physicalSourceTypes.add(memorySourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("MemorySourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = getTestResourceFolder() / "memorySourceTestOut.csv";
    remove(filePath.c_str());

    //register query
    auto query = Query::from("memory_stream").sink(FileSinkDescriptor::create(filePath, "CSV_FORMAT", "APPEND"));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("MemorySourceIntegrationTest: Remove query");
    //ASSERT_TRUE(requestHandlerService->validateAndQueueStopQueryRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //    NES_INFO("MemorySourceIntegrationTest: content={}", content);
    ASSERT_TRUE(!content.empty());

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
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/// This test checks that a deployed MemorySource can write M records stored in one buffer that is not full
TEST_F(MemorySourceIntegrationTest, testMemorySourceFewTuples) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MemorySourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MemorySourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    struct Record {
        uint64_t key;
        uint64_t timestamp;
    };
    static_assert(sizeof(Record) == 16);

    auto schema = Schema::create()
                      ->addField("key", DataTypeFactory::createUInt64())
                      ->addField("timestamp", DataTypeFactory::createUInt64());
    ASSERT_EQ(schema->getSchemaSizeInBytes(), sizeof(Record));

    sourceCatalog->addLogicalSource("memory_stream", schema);

    NES_INFO("MemorySourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    wrkConf->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    constexpr auto memAreaSize = sizeof(Record) * 5;
    //constexpr auto bufferSizeInNodeEngine = 4096;// TODO load this from config!
    constexpr auto buffersToExpect = 1;
    auto recordsToExpect = memAreaSize / schema->getSchemaSizeInBytes();
    auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));
    auto* records = reinterpret_cast<Record*>(memArea);
    size_t recordSize = schema->getSchemaSizeInBytes();
    size_t numRecords = memAreaSize / recordSize;
    for (auto i = 0U; i < numRecords; ++i) {
        records[i].key = i;
        records[i].timestamp = i;
    }

    auto memorySourceType =
        MemorySourceType::create("memory_stream", "memory_stream_0", memArea, memAreaSize, 1, 0, GatheringMode::INTERVAL_MODE);
    wrkConf->physicalSourceTypes.add(memorySourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("MemorySourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = getTestResourceFolder() / "memorySourceTestOut.csv";
    remove(filePath.c_str());

    //register query
    auto query = Query::from("memory_stream").sink(FileSinkDescriptor::create(filePath, "CSV_FORMAT", "APPEND"));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("MemorySourceIntegrationTest: Remove query");
    //ASSERT_TRUE(requestHandlerService->validateAndQueueStopQueryRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("MemorySourceIntegrationTest: content={}", content);
    ASSERT_TRUE(!content.empty());

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
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/// This test checks that a deployed MemorySource can write M records stored in N+1 buffers
/// with the invariant that the N+1-th buffer is half full
TEST_F(MemorySourceIntegrationTest, testMemorySourceHalfFullBuffer) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MemorySourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MemorySourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    struct Record {
        uint64_t key;
        uint64_t timestamp;
    };
    static_assert(sizeof(Record) == 16);

    auto schema = Schema::create()
                      ->addField("key", DataTypeFactory::createUInt64())
                      ->addField("timestamp", DataTypeFactory::createUInt64());
    ASSERT_EQ(schema->getSchemaSizeInBytes(), sizeof(Record));

    sourceCatalog->addLogicalSource("memory_stream", schema);

    NES_INFO("MemorySourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    wrkConf->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;

    // We want to test here, if 1.5 buffers can be send.
    const auto bufferSizeInNodeEngine = wrkConf->bufferSizeInBytes.getValue();
    const auto memAreaSize = bufferSizeInNodeEngine + (bufferSizeInNodeEngine / 2);
    const auto buffersToExpect = memAreaSize / bufferSizeInNodeEngine;
    auto recordsToExpect = memAreaSize / schema->getSchemaSizeInBytes();
    auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));
    auto* records = reinterpret_cast<Record*>(memArea);
    const auto recordSize = schema->getSchemaSizeInBytes();
    const auto numRecords = memAreaSize / recordSize;
    const auto bufferCapacity = bufferSizeInNodeEngine / recordSize;
    NES_DEBUG("Creating {} records and a capacity of {} records...", numRecords, bufferCapacity);
    for (auto i = 0U; i < numRecords; ++i) {
        records[i].key = i;
        records[i].timestamp = i;
    }

    auto memorySourceType = MemorySourceType::create("memory_stream",
                                                     "memory_stream_0",
                                                     memArea,
                                                     memAreaSize,
                                                     buffersToExpect + 1,
                                                     0,
                                                     GatheringMode::INTERVAL_MODE);

    wrkConf->physicalSourceTypes.add(memorySourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("MemorySourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = getTestResourceFolder() / "memorySourceTestOut";
    remove(filePath.c_str());

    //register query
    auto query = Query::from("memory_stream").sink(FileSinkDescriptor::create(filePath, "CSV_FORMAT", "APPEND"));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("MemorySourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    const std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    ASSERT_TRUE(!content.empty());

    std::ifstream infile(filePath.c_str());
    std::string line;
    std::size_t lineCnt = 0;
    while (std::getline(infile, line)) {
        if (lineCnt > 0) {
            const auto expectedString = std::to_string(lineCnt - 1) + "," + std::to_string(lineCnt - 1);
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
