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

#include <gtest/gtest.h>
#include "../util/NesBaseTest.hpp"

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;


class MultipleJoinsTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MultipleJoinsTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MultipleJoinsTest test class.");
    }

    std::string ipAddress = "127.0.0.1";
};

TEST_F(MultipleJoinsTest, testJoins2WithDifferentStreamTumblingWindowOnCoodinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setSourceFrequency(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setSourceFrequency(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setSourceFrequency(1);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamTumblingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

/**
 * @brief This tests just outputs the default stream for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0] => Join 2
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0] => Join 1
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(MultipleJoinsTest, DISABLED_testJoin2WithDifferentStreamTumblingWindowDistributed) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(*rpcCoordinatorPort);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    auto physicalSource4 = PhysicalSource::create("window3", "test_stream", csvSourceType4);
    workerConfig4->addPhysicalSource(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1, 2);
    NES_INFO("MultipleJoinsTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamTumblingWindowDistributed.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentStreamTumblingWindowOnCoodinatorSequential) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", UINT64))->addField(createField("id4", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window4", window4);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    auto physicalSource4 = PhysicalSource::create("window4", "test_stream", csvSourceType4);
    workerConfig4->addPhysicalSource(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("MultipleJoinsTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testJoin4WithDifferentStreamTumblingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .joinWith(Query::from("window4")).where(Attribute("id1")).equalsTo(Attribute("id4")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,"
        "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:"
        "INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:"
        "INTEGER,window3$id3:INTEGER,window3$timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:"
        "INTEGER\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentStreamTumblingWindowOnCoodinatorNested) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", UINT64))->addField(createField("id4", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window4", window4);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->setCoordinatorPort(*rpcCoordinatorPort);
    auto csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    auto physicalSource4 = PhysicalSource::create("window4", "test_stream", csvSourceType4);
    workerConfig4->addPhysicalSource(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("MultipleJoinsTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testJoin4WithDifferentStreamTumblingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    auto query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .joinWith((Query::from("window3")).joinWith(Query::from("window4")).where(Attribute("id3")).equalsTo(Attribute("id4")).window(TumblingWindow::of(EventTime(Attribute("timestamp"))
        ,Milliseconds(1000)))).where(Attribute("id1")).equalsTo(Attribute("id4")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$"
        "id1:INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3window4$"
        "start:INTEGER,window3window4$end:INTEGER,window3window4$key:INTEGER,window3$win3:INTEGER,window3$id3:INTEGER,window3$"
        "timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

/**
 *
 *
 * Sliding window joins
 *
 */

TEST_F(MultipleJoinsTest, testJoins2WithDifferentStreamSlidingWindowOnCoodinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(port);
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamSlidingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

/**
 * @brief This tests just outputs the default stream for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0] => Join 2
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0] => Join 1
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(MultipleJoinsTest, DISABLED_testJoin2WithDifferentStreamSlidingWindowDistributed) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(port);
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->setCoordinatorPort(port);
    auto csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    auto physicalSource4 = PhysicalSource::create("window3", "test_stream", csvSourceType4);
    workerConfig4->addPhysicalSource(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1, 2);
    NES_INFO("MultipleJoinsTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamSlidingWindowDistributed.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentStreamSlidingWindowOnCoodinatorSequential) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", UINT64))->addField(createField("id4", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window4", window4);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(port);
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->setCoordinatorPort(port);
    auto csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    auto physicalSource4 = PhysicalSource::create("window4", "test_stream", csvSourceType4);
    workerConfig4->addPhysicalSource(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("MultipleJoinsTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testJoin4WithDifferentStreamSlidingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window4")).where(Attribute("id1")).equalsTo(Attribute("id4")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,"
        "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:"
        "INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:"
        "INTEGER,window3$id3:INTEGER,window3$timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:"
        "INTEGER\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "1000,2000,12,1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentStreamSlidingWindowOnCoodinatorNested) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", UINT64))->addField(createField("id4", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window4", window4);
    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(port);
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("MultipleJoinsTest: Worker3 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->setCoordinatorPort(port);
    auto csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    auto physicalSource4 = PhysicalSource::create("window4", "test_stream", csvSourceType4);
    workerConfig4->addPhysicalSource(physicalSource4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("MultipleJoinsTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testJoin4WithDifferentStreamSlidingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    auto query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith((Query::from("window3")).joinWith(Query::from("window4")).where(Attribute("id3")).equalsTo(Attribute("id4")).window(SlidingWindow::of(EventTime(Attribute("timestamp"))
        ,Seconds(1),Milliseconds(500)))).where(Attribute("id1")).equalsTo(Attribute("id4")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$"
        "id1:INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3window4$"
        "start:INTEGER,window3window4$end:INTEGER,window3window4$key:INTEGER,window3$win3:INTEGER,window3$id3:INTEGER,window3$"
        "timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}
}// namespace NES
