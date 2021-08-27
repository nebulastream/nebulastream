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

#ifdef ENABLE_MQTT_BUILD
#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <cstring>
#include <future>
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <Runtime/NodeEngineFactory.hpp>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <thread>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestUtils.hpp>

#ifndef SERVERADDRESS
#define SERVERADDRESS "tcp://127.0.0.1:1883"
#endif

#ifndef CLIENTID
#define CLIENTID "nes-mqtt-test-client"
#endif

#ifndef TOPIC
#define TOPIC "v1/devices/me/telemetry"
#endif

#ifndef USER
#define USER "rfRqLGZRChg8eS30PEeR"
#endif

#ifndef OPERATORID
#define OPERATORID 1
#endif

#ifndef NUMSOURCELOCALBUFFERS
#define NUMSOURCELOCALBUFFERS 12
#endif

#ifndef SUCCESSORS
#define SUCCESSORS {}
#endif

#ifndef DATATYPE
#define DATATYPE MQTTSourceDescriptor::DataType::JSON
#endif

#ifndef QOS
#define QOS 1
#endif

#ifndef CLEANSESSION
#define CLEANSESSION false
#endif

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

namespace NES {

class MQTTSourceTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("MQTTSourceTest.log", NES::LOG_DEBUG);
        NES_DEBUG("MQTTSOURCETEST::SetUpTestCase()");
    }

    void SetUp() override {
        NES_DEBUG("MQTTSOURCETEST::SetUp() MQTTSourceTest cases set up.");

        test_schema = Schema::create()->addField("var", UINT32);

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, conf);

        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        nodeEngine->stop();
        nodeEngine.reset();
        NES_DEBUG("MQTTSOURCETEST::TearDown() Tear down MQTTSourceTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("MQTTSOURCETEST::TearDownTestCases() Tear down MQTTSourceTest test class."); }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Runtime::BufferManagerPtr bufferManager;
    Runtime::QueryManagerPtr queryManager;
    SchemaPtr test_schema;
    uint64_t buffer_size{};
};

/**
 * Tests basic set up of MQTT source
 */
TEST_F(MQTTSourceTest, MQTTSourceInit) {

    auto mqttSource = createMQTTSource(test_schema,
                                       bufferManager,
                                       queryManager,
                                       SERVERADDRESS,
                                       CLIENTID,
                                       USER,
                                       TOPIC,
                                       OPERATORID,
                                       NUMSOURCELOCALBUFFERS,
                                       SUCCESSORS,
                                       DATATYPE,
                                       QOS,
                                       CLEANSESSION);

    SUCCEED();
}

/**
 * Test if schema, MQTT server address, clientId, user, and topic are the same
 */
TEST_F(MQTTSourceTest, MQTTSourcePrint) {

    auto mqttSource = createMQTTSource(test_schema,
                                       bufferManager,
                                       queryManager,
                                       SERVERADDRESS,
                                       CLIENTID,
                                       USER,
                                       TOPIC,
                                       OPERATORID,
                                       NUMSOURCELOCALBUFFERS,
                                       SUCCESSORS,
                                       DATATYPE,
                                       QOS,
                                       CLEANSESSION);

    std::string expected = "MQTTSOURCE(SCHEMA(var:INTEGER ), SERVERADDRESS=tcp://127.0.0.1:1883, "
                           "CLIENTID=nes-mqtt-test-client, "
                           "USER=rfRqLGZRChg8eS30PEeR, TOPIC=v1/devices/me/telemetry, "
                           "DATATYPE=0, QOS=1, CLEANSESSION=0. ";

    EXPECT_EQ(mqttSource->toString(), expected);

    std::cout << mqttSource->toString() << std::endl;

    SUCCEED();
}

/**
 * Tests if obtained value is valid.
 */
TEST_F(MQTTSourceTest, DISABLED_MQTTSourceValue) {

    auto test_schema = Schema::create()->addField("var", UINT32);
    auto mqttSource =
        createMQTTSource(test_schema, bufferManager, queryManager, SERVERADDRESS, CLIENTID, USER, TOPIC, OPERATORID,
                         NUMSOURCELOCALBUFFERS,
                         SUCCESSORS,
                         DATATYPE,
                         QOS,
                         CLEANSESSION);
    auto tuple_buffer = mqttSource->receiveData();
    EXPECT_TRUE(tuple_buffer.has_value());
    uint64_t value = 0;
    auto* tuple = (uint32_t*) tuple_buffer->getBuffer();
    value = *tuple;
    uint64_t expected = 43;
    NES_DEBUG("MQTTSOURCETEST::TEST_F(MQTTSourceTest, MQTTSourceValue) expected value is: " << expected
                                                                                            << ". Received value is: " << value);
    EXPECT_EQ(value, expected);
}

TEST_F(MQTTSourceTest, testDeployOneWorkerWithMQTTSourceConfig) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    remove("test.out");

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    //register logical stream qnv
    std::string stream =
        R"(Schema::create()->addField("type", DataTypeFactory::createArray(10, DataTypeFactory::createChar()))
                            ->addField(createField("hospitalId", UINT64))
                            ->addField(createField("stationId", UINT64))
                            ->addField(createField("patientId", UINT64))
                            ->addField(createField("time", UINT64))
                            ->addField(createField("healthStatus", UINT8))
                            ->addField(createField("healthStatusDuration", UINT32))
                            ->addField(createField("recovered", BOOLEAN))
                            ->addField(createField("dead", BOOLEAN));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << stream;
    out.close();
    wrk1->registerLogicalStream("stream", testSchemaFileName);

    srcConf->setSourceType("MQTTSource");
    //0 = serverAddress; 1 = clientId; 2 = user; 3 = topic; 4 = dataType (conversion to enum above)
//    srcConf->setSourceConfig("../tests/test_data/window.csv");
srcConf->setSourceConfig("ws://127.0.0.1:9001;testClient;testUser;demoTownSensorData;JSON;2;true");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setNumberOfBuffersToProduce(10000);
    srcConf->setSourceFrequency(1);//TODO: change this to 0 if we have the the end of stream
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("stream");
    srcConf->setSkipHeader(true);
    //register physical stream
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(streamConf);

    std::string outputFilePath = "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream").filter(Attribute("hospitalId") < 5).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
//    int response = remove("test.out");
//    EXPECT_TRUE(response == 0);
}
}// namespace NES
#endif
