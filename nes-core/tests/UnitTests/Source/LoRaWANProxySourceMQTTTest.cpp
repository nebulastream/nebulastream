// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//         https://www.apache.org/licenses/LICENSE-2.0
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// *

#include <Runtime/NodeEngineBuilder.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp>
#include <BaseIntegrationTest.hpp>
#include <Services/QueryService.hpp>
#include <Sources/SourceCreator.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <mqtt/client.h>
constexpr int OPERATORID = 1;
constexpr int ORIGINID = 1;
constexpr int NUMSOURCELOCALBUFFERS = 12;

const std::string ADDRESS{"tcp://localhost:1883"};
const std::string PRODUCER_CLIENT_ID{"producer_client"};
const std::string CONSUMER_CLIENT_ID{"consumer_client"};
const std::string DEVICE_ID = "1122334455667788";
const std::string APP_ID = "0102030405060708";
const std::string TOPIC = "application/" + APP_ID + "/device/" + DEVICE_ID + "/event/up";

namespace NES {
class LoRaWANProxySourceMQTTTest : public Testing::BaseIntegrationTest {
  public:
    mqtt::client client{ADDRESS, PRODUCER_CLIENT_ID};

    std::map<std::string, std::string> sourceConfig{
        {Configurations::LORAWAN_NETWORK_STACK_CONFIG, "ChirpStack"},
        {Configurations::URL_CONFIG, ADDRESS},
        {Configurations::USER_NAME_CONFIG, CONSUMER_CLIENT_ID},
        {Configurations::PASSWORD_CONFIG, "hellothere"},
        {Configurations::LORAWAN_APP_ID_CONFIG, APP_ID},
        {Configurations::LORAWAN_CA_PATH, ""},
        {Configurations::LORAWAN_CERT_PATH,""},
        {Configurations::LORAWAN_KEY_PATH, ""},
        {Configurations::LORAWAN_DEVICE_EUIS, {"70b3d549938ea1ee",}},
        {Configurations::LORAWAN_SENSOR_FIELDS,{"temperature","acceleration"}}
    };
    LoRaWANProxySourceTypePtr loRaWANProxySourceType;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        Testing::BaseIntegrationTest::SetUpTestCase();
        NES::Logger::setupLogging("LoRaWANProxySourceMQTTTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("LORAWANPROXYSOURCEMQTTTEST::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("LORAWANPROXYSOURCEMQTTTEST::SetUp() LoRaWANProxySource MQTT test cases set up.");
        client.connect();
        ASSERT_TRUE(client.is_connected()) << "client setup failed";

        schema = Schema::create()->addField("var", BasicType::UINT32);
        loRaWANProxySourceType = LoRaWANProxySourceType::create(sourceConfig);
        auto workerConfigurations = WorkerConfiguration::create();
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();
    }

    void TearDown() override {
        Testing::BaseIntegrationTest::TearDown();
        client.disconnect();
        ASSERT_TRUE(nodeEngine->stop());
        NES_DEBUG("LORAWANPROXYSOURCEMQTTTEST::TearDown() Tear down LoRaWANProxySourceMQTTTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        Testing::BaseIntegrationTest::TearDownTestCase();
        NES_DEBUG("LORAWANPROXYSOURCEMQTTTEST::TearDownTestCases() Tear down LoRaWANProxySourceMQTTTest test class.");
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Runtime::BufferManagerPtr bufferManager;
    Runtime::QueryManagerPtr queryManager;
    SchemaPtr schema;
    uint64_t buffer_size{};
};

TEST_F(LoRaWANProxySourceMQTTTest, LoRaWANProxySourceConnect) {
    auto init = LoRaWANProxySource(schema,
                                   bufferManager,
                                   queryManager,
                                   loRaWANProxySourceType,
                                   OPERATORID,
                                   ORIGINID,
                                   NUMSOURCELOCALBUFFERS,
                                   GatheringMode::INTERVAL_MODE,
                                   {});
    EXPECT_TRUE(init.connect());
};

TEST_F(LoRaWANProxySourceMQTTTest, LoRaWANProxySourceRecieveData) {
    auto init = LoRaWANProxySource(schema,
                                   bufferManager,
                                   queryManager,
                                   loRaWANProxySourceType,
                                   OPERATORID,
                                   ORIGINID,
                                   NUMSOURCELOCALBUFFERS,
                                   GatheringMode::INTERVAL_MODE,
                                   {});
    init.open();
    EXPECT_TRUE(init.connect());
    sleep(1);
    auto msg = mqtt::make_message(TOPIC, R"({ "var" : 1337 })");
    client.publish(msg);
    sleep(1);
    auto data = init.receiveData();
    EXPECT_TRUE(data.has_value());
    uint32_t tuple = (*(uint32_t*) data->getBuffer());
    EXPECT_EQ(tuple, 1337);
};

TEST_F(LoRaWANProxySourceMQTTTest, LoRaWANProxySourceDeployOneWorkerWithSourceConfig) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create(0,nullptr);
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->numWorkerThreads = 4;
    wrkConf->logLevel = LogLevel::LOG_TRACE;

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->logLevel = LogLevel::LOG_TRACE;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    client.publish(mqtt::make_message(TOPIC, R"({ "value": 1 })"));
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    std::string source = R"(Schema::create()->addField(createField("value", UINT8));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream", source);

    auto sourceType = LoRaWANProxySourceType::create();
    sourceType->setUrl(ADDRESS);
    sourceType->setAppId(APP_ID);
    sourceType->setUserName("testUser");
    auto physicalSource = PhysicalSource::create("stream", "test_stream", sourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));

    EXPECT_TRUE(wrk1->start(false, true));

    // send some data
    client.publish(mqtt::make_message(TOPIC, R"({ "value": 2 })"));
    //client.publish(mqtt::make_message(TOPIC, R"({ "type" : ['h','e','l','l','o','t','h','e','r','e'], "value": 2 })"));
    //client.publish(mqtt::make_message(TOPIC, R"({ "type" : ['g','e','n','e','r','a','l',' ','g','r'], "value": 3 })"));
    sleep(1);
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = "/tmp/test.out";
    string query =
        R"(Query::from("stream").sink(FileSinkDescriptor::create(")" + outputFilePath + R"(", "TEXT_FORMAT", "APPEND"));)";
    string query2 = R"(Query::from("stream").filter(Attribute("value") < 3).sink(PrintSinkDescriptor::create());)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::TopDown, FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    sleep(5);
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1));
    NES_INFO("\n\n --------- CONTENT --------- \n\n");
    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_INFO("%s",content);

    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    EXPECT_TRUE(wrk1->stop(true));

    EXPECT_TRUE(crd->stopCoordinator(true));
};

}// namespace NES
