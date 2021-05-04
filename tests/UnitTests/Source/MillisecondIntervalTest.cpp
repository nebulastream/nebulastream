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

#include <chrono>
#include <cmath>
#include <thread>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Services/QueryService.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

namespace NES {

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

/**
 * This test set holds the corner cases for moving our sampling frequencies to
 * sub-second intervals. Before, NES was sampling every second and was checking
 * every second if that future timestamp is now stale (older).
 *
 * First we check for sub-second unit-tests on a soruce and its behavior. Then,
 * we include an E2Etest with a stream that samples at sub-second interval.
 */
class MillisecondIntervalTest : public testing::Test {
  public:
    CoordinatorConfigPtr crdConf;
    WorkerConfigPtr wrkConf;
    SourceConfigPtr srcConf;

    static void SetUpTestCase() {
        NES::setupLogging("MillisecondIntervalTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MillisecondIntervalTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MillisecondIntervalTest test class."); }

    void SetUp() {

        restPort = restPort + 3;
        rpcPort = rpcPort + 40;

        crdConf = CoordinatorConfig::create();
        crdConf->setRpcPort(rpcPort);
        crdConf->setRestPort(restPort);

        wrkConf = WorkerConfig::create();
        wrkConf->setCoordinatorPort(rpcPort);

        srcConf = SourceConfig::create();
        srcConf->setSourceType("DefaultSource");
        srcConf->setSourceConfig("../tests/test_data/exdra.csv");
        srcConf->setSourceFrequency(550);
        srcConf->setNumberOfTuplesToProducePerBuffer(1);
        srcConf->setNumberOfBuffersToProduce(3);
        srcConf->setPhysicalStreamName("physical_test");
        srcConf->setLogicalStreamName("testStream");

        NES_INFO("Setup MillisecondIntervalTest class.");
    }

    void TearDown() { NES_INFO("Tear down MillisecondIntervalTest test case."); }

};// FractionedIntervalTest

TEST_F(MillisecondIntervalTest, DISABLED_testCSVSourceWithOneLoopOverFileSubSecond) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    std::string path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.csv";

    const std::string& del = ",";
    double frequency = 550;
    SchemaPtr schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", UINT64)
                           ->addField("ip", INT32);

    uint64_t numberOfBuffers = 5;

    const DataSourcePtr source = createCSVFileSource(schema,
                                                     nodeEngine->getBufferManager(),
                                                     nodeEngine->getQueryManager(),
                                                     path_to_file,
                                                     del,
                                                     0,
                                                     numberOfBuffers,
                                                     frequency,
                                                     false,
                                                     1,
                                                     12,
                                                     {});
    source->start();

    for (uint64_t i = 0; i < numberOfBuffers; i++) {
        auto optBuf = source->receiveData();
    }

    uint64_t expectedNumberOfTuples = 260;
    EXPECT_EQ(source->getNumberOfGeneratedTuples(), expectedNumberOfTuples);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numberOfBuffers);
    EXPECT_EQ(source->getGatheringIntervalCount(), frequency);
}

TEST_F(MillisecondIntervalTest, DISABLED_testMultipleOutputBufferFromDefaultSourcePrintSubSecond) {

    crdConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();

    NES_INFO("MillisecondIntervalTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MillisecondIntervalTest: Coordinator started successfully");

    NES_INFO("MillisecondIntervalTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MillisecondIntervalTest: Worker1 started successfully");

    //register logical stream
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("testStream", testSchemaFileName);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);

    //register physical stream
    wrk1->registerPhysicalStream(conf);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register query
    std::string queryString =
        "Query::from(\"testStream\").filter(Attribute(\"campaign_id\") < 42).sink(PrintSinkDescriptor::create());";

    QueryId queryId = queryService->validateAndQueueAddRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("MillisecondIntervalTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}//namespace NES