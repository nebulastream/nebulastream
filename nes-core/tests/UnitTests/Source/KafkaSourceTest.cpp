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

#include <NesBaseTest.hpp>
#include <gtest/gtest.h>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/SourceCreator.hpp>
#include <cstring>
#include <string>
#include <thread>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>


#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <gtest/gtest.h>
#include <Sinks/Mediums//KafkaSink.hpp>
#include <Sources/KafkaSource.hpp>
#include <Util/Logger/Logger.hpp>

#include <Services/QueryService.hpp>
#include <Util/TestUtils.hpp>
#include <Util/TimeMeasurement.hpp>

#ifndef OPERATORID
#define OPERATORID 1
#endif

#ifndef NUMSOURCELOCALBUFFERS
#define NUMSOURCELOCALBUFFERS 12
#endif

const std::string KAFKA_BROKER = "localhost:9092";

namespace NES {

class KafkaSourceTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KAFKASourceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("KAFKASOURCETEST::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_DEBUG("KAFKASOURCETEST::SetUp() KAFKASourceTest cases set up.");
        test_schema = Schema::create()->addField("var", UINT32);
        kafkaSourceType = KafkaSourceType::create();
        auto workerConfigurations = WorkerConfiguration::create();
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        Testing::NESBaseTest::TearDown();
        ASSERT_TRUE(nodeEngine->stop());
        NES_DEBUG("KAFKASOURCETEST::TearDown() Tear down MQTTSourceTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("KAFKASOURCETEST::TearDownTestCases() Tear down KAFKASourceTest test class."); }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Runtime::BufferManagerPtr bufferManager;
    Runtime::QueryManagerPtr queryManager;
    SchemaPtr test_schema;
    uint64_t buffer_size{};
    KafkaSourceTypePtr kafkaSourceType;

    const std::string brokers = std::string(KAFKA_BROKER);
    const std::string topic = std::string("nes");
    const std::string groupId = std::string("nes");
};


/**
 * Tests basic set up of Kafka source
 */
TEST_F(KafkaSourceTest, KafkaSourceInit) {

    auto kafkaSource = createKafkaSource(test_schema,
                                         bufferManager,
                                         queryManager,
                                         brokers,
                                         topic,
                                         groupId,
                                         true,
                                         100,
                                         OPERATORID,
                                         NUMSOURCELOCALBUFFERS);

    SUCCEED();
}

/**
 * Test if schema, Kafka server address, clientId, user, and topic are the same
 */
TEST_F(KafkaSourceTest, KafkaSourcePrint) {

    auto kafkaSource = createKafkaSource(test_schema,
                                         bufferManager,
                                         queryManager,
                                         brokers,
                                         topic,
                                         groupId,
                                         true,
                                         100,
                                         OPERATORID,
                                         NUMSOURCELOCALBUFFERS);

    std::string expected = "KAFKASOURCE(SCHEMA(var:INTEGER), BROKER=localhost:9092, "
                           "TOPIC(nes). ";

    EXPECT_EQ(kafkaSource->toString(), expected);

    std::cout << kafkaSource->toString() << std::endl;

    SUCCEED();
}

/**
 * Tests if obtained value is valid.
 */
TEST_F(KafkaSourceTest, KafkaSourceValue) {

    auto test_schema = Schema::create()->addField("var", UINT32);
    auto kafkaSource = createKafkaSource(test_schema,
                                         bufferManager,
                                         queryManager,
                                         brokers,
                                         topic,
                                         groupId,
                                         true,
                                         100,
                                         OPERATORID,
                                         NUMSOURCELOCALBUFFERS);
    auto tuple_buffer = kafkaSource->receiveData();
    EXPECT_TRUE(tuple_buffer.has_value());
    uint64_t value = 0;
    auto* tuple = (uint32_t*) tuple_buffer->getBuffer();
    value = *tuple;
    uint64_t expected = 43;
    NES_DEBUG("KAFKASOURCETEST::TEST_F(KAFKASourceTest, KAFKASourceValue) expected value is: " << expected
                                                                                            << ". Received value is: " << value);
    EXPECT_EQ(value, expected);
}

// Disabled, because it requires a manually set up Kafka broker
TEST_F(KafkaSourceTest, DISABLED_testDeployOneWorkerWithKafkaSourceConfig) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("KAFKASOURCETEST:: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    std::string source =
        R"(Schema::create()->addField("type", DataTypeFactory::createArray(10, DataTypeFactory::createChar()))
                            ->addField(createField("hospitalId", UINT64))
                            ->addField(createField("stationId", UINT64))
                            ->addField(createField("patientId", UINT64))
                            ->addField(createField("time", UINT64))
                            ->addField(createField("healthStatus", UINT8))
                            ->addField(createField("healthStatusDuration", UINT32))
                            ->addField(createField("recovered", BOOLEAN))
                            ->addField(createField("dead", BOOLEAN));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream", source);
    NES_INFO("KAFKASOURCETEST:: Coordinator started successfully");

    NES_INFO("KAFKASOURCETEST:: Start worker 1");
    wrkConf->coordinatorPort = port;
    kafkaSourceType->setBrokers(KAFKA_BROKER);
    kafkaSourceType->setTopic(topic);
    kafkaSourceType->setGroupId(groupId);
    kafkaSourceType->setAutoCommit(true);
    kafkaSourceType->setConnectionTimeout(100);
    auto physicalSource = PhysicalSource::create("stream", "test_stream", kafkaSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream").filter(Attribute("hospitalId") < 5).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

}// namespace NES
