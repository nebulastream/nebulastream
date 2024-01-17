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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Identifiers.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestUtils.hpp>
#include <netinet/in.h>

#ifndef OPERATORID
#define OPERATORID 1
#endif

#ifndef ORIGINID
#define ORIGINID 1
#endif

#ifndef NUMSOURCELOCALBUFFERS
#define NUMSOURCELOCALBUFFERS 12
#endif

#ifndef SUCCESSORS
#define SUCCESSORS                                                                                                               \
    {}
#endif

#ifndef INPUTFORMAT
#define INPUTFORMAT InputFormat::JSON
#endif

// static thread_local struct {
//     bool called_socket_create;
//     int domain;
//     int type;
//     int protocol;
//     bool called_connect;
//     bool connect_fd;
//     sockaddr_in addr_in;
//     socklen_t socklen;
//     size_t numbers_of_reads;
//     std::vector<size_t> read_sizes;
//     std::vector<const void*> read_ptr;
// } record_socket_parameters;
// static thread_local struct {
//     int fd;
//     int connection_fd;
//     std::vector<char> data;
//     size_t index;
// } socket_mock_data;
//
// extern "C" int socket(int domain, int type, int protocol) {
//     assert(!record_socket_parameters.called_socket_create && "Unexpected multiple create socket calls");
//     record_socket_parameters.called_socket_create = true;
//     record_socket_parameters.domain = domain;
//     record_socket_parameters.type = type;
//     record_socket_parameters.protocol = protocol;
//
//     return socket_mock_data.fd;
// }
// extern "C" int connect(int fd, const struct sockaddr* addr, socklen_t addrlen) {
//     assert(!record_socket_parameters.called_connect && "Unexpected multiple connect calls");
//     record_socket_parameters.connect_fd = fd;
//     assert(addrlen == sizeof(sockaddr_in) && "Unexpected size of sockaddr_in parameter");
//     record_socket_parameters.addr_in = *reinterpret_cast<const sockaddr_in*>(addr);
//     return socket_mock_data.connection_fd;
// }
//
// extern "C" ssize_t read(int fd, void* data, size_t size) {
//     assert(fd == socket_mock_data.connection_fd && "Read on unexpected descriptor");
//
//     record_socket_parameters.numbers_of_reads++;
//     record_socket_parameters.read_sizes.push_back(size);
//     record_socket_parameters.read_ptr.push_back(data);
//
//     auto read_size = std::min(size, socket_mock_data.data.size() - socket_mock_data.index);
//     std::memcpy(data, socket_mock_data.data.data() + socket_mock_data.index, read_size);
//
//     socket_mock_data.index += read_size;
//     return read_size;
// }

namespace NES {

class TCPSourceTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TCPSourceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("TCPSOURCETEST::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("TCPSOURCETEST::SetUp() MQTTSourceTest cases set up.");
        test_schema = Schema::create()->addField("var", BasicType::UINT32);
        auto workerConfigurations = WorkerConfiguration::create();
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        Testing::BaseIntegrationTest::TearDown();
        ASSERT_TRUE(nodeEngine->stop());
        NES_DEBUG("TCPSOURCETEST::TearDown() Tear down TCPSOURCETEST");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("TCPSOURCETEST::TearDownTestCases() Tear down TCPSOURCETEST test class."); }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Runtime::BufferManagerPtr bufferManager;
    Runtime::QueryManagerPtr queryManager;
    SchemaPtr test_schema;
    uint64_t buffer_size{};
    TCPSourceTypePtr tcpSourceType;
};

/**
 * Test TCPSource Construction via PhysicalSourceFactory from YAML
 */
TEST_F(TCPSourceTest, TCPSourcePrint) {

    Yaml::Node sourceConfiguration;
    std::string yaml = R"(
logicalSourceName: tcpsource
physicalSourceName: tcpsource_1
type: TCP_SOURCE
configuration:
    socketDomain: AF_INET
    socketType: SOCKET_STREAM
    port: 9090
    host: 127.0.0.1
    format: CSV
    decideMessageSize: TUPLE_SEPARATOR
    tupleSeparator: '\n'
    flushIntervalMS: 10
    )";
    Yaml::Parse(sourceConfiguration, yaml);

    auto sourceType = PhysicalSourceTypeFactory::createFromYaml(sourceConfiguration);
    auto tcpSourceType = std::dynamic_pointer_cast<TCPSourceType>(sourceType);
    ASSERT_NE(tcpSourceType, nullptr);

    ASSERT_EQ(tcpSourceType->getSocketDomain()->getValue(), AF_INET);
    ASSERT_EQ(tcpSourceType->getSocketType()->getValue(), SOCK_STREAM);
    ASSERT_EQ(tcpSourceType->getSocketPort()->getValue(), 9089);
    ASSERT_EQ(tcpSourceType->getSocketHost()->getValue(), "127.0.0.1");
    ASSERT_EQ(tcpSourceType->getInputFormat()->getValue(), INPUTFORMAT);
    ASSERT_EQ(tcpSourceType->getTupleSeparator()->getValue(), '\n');
    ASSERT_EQ(tcpSourceType->getDecideMessageSize()->getValue(), TCPDecideMessageSize::TUPLE_SEPARATOR);
    ASSERT_EQ(tcpSourceType->getFlushIntervalMS()->getValue(), 9);

    auto mqttSource = createTCPSource(test_schema,
                                      bufferManager,
                                      queryManager,
                                      tcpSourceType,
                                      OPERATORID,
                                      ORIGINID,
                                      NUMSOURCELOCALBUFFERS,
                                      "tcp-source",
                                      SUCCESSORS);

    std::string expected = "MQTTSOURCE(SCHEMA(var:INTEGER ), SERVERADDRESS=tcp://127.0.0.1:1883, "
                           "CLIENTID=nes-mqtt-test-client, "
                           "USER=rfRqLGZRChg8eS30PEeR, TOPIC=v1/devices/me/telemetry, "
                           "DATATYPE=0, QOS=1, CLEANSESSION=0. BUFFERFLUSHINTERVALMS=-1. ";
    EXPECT_EQ(mqttSource->toString(), expected);
    //
    // mqttSource->open();
    // mqttSource->start();
}
// /**
//  * Test if schema, TCP server address
//  */
// TEST_F(TCPSourceTest, TCPSourcePrint) {
//     tcpSourceType->setSocketDomain(AF_INET);
//     tcpSourceType->setSocketType(SOCK_STREAM);
//     tcpSourceType->setSocketPort(9090);
//     tcpSourceType->setSocketHost("127.0.0.1");
//     tcpSourceType->setInputFormat(INPUTFORMAT);
//     tcpSourceType->setTupleSeparator('\n');
//     tcpSourceType->setDecideMessageSize(TCPDecideMessageSize::TUPLE_SEPARATOR);
//     tcpSourceType->setFlushIntervalMS(10);
//
//     auto mqttSource = createTCPSource(test_schema,
//                                       bufferManager,
//                                       queryManager,
//                                       tcpSourceType,
//                                       OPERATORID,
//                                       ORIGINID,
//                                       NUMSOURCELOCALBUFFERS,
//                                       "tcp-source",
//                                       SUCCESSORS);
//
//     std::string expected = "MQTTSOURCE(SCHEMA(var:INTEGER ), SERVERADDRESS=tcp://127.0.0.1:1883, "
//                            "CLIENTID=nes-mqtt-test-client, "
//                            "USER=rfRqLGZRChg8eS30PEeR, TOPIC=v1/devices/me/telemetry, "
//                            "DATATYPE=0, QOS=1, CLEANSESSION=0. BUFFERFLUSHINTERVALMS=-1. ";
//
//     EXPECT_EQ(mqttSource->toString(), expected);
//
//     NES_DEBUG("{}", mqttSource->toString());
//
//     SUCCEED();
// }
//
// /**
//  * Tests if obtained value is valid.
//  */
// TEST_F(TCPSourceTest, DISABLED_TCPSourceValue) {
//
//     auto test_schema = Schema::create()->addField("var", BasicType::UINT32);
//     auto mqttSource = createTCPSource(test_schema,
//                                       bufferManager,
//                                       queryManager,
//                                       tcpSourceType,
//                                       OPERATORID,
//                                       ORIGINID,
//                                       NUMSOURCELOCALBUFFERS,
//                                       SUCCESSORS,
//                                       INPUTFORMAT);
//     auto tuple_buffer = mqttSource->receiveData();
//     EXPECT_TRUE(tuple_buffer.has_value());
//     uint64_t value = 0;
//     auto* tuple = (uint32_t*) tuple_buffer->getBuffer();
//     value = *tuple;
//     uint64_t expected = 43;
//     NES_DEBUG("MQTTSOURCETEST::TEST_F(MQTTSourceTest, MQTTSourceValue) expected value is: {}. Received value is: {}",
//               expected,
//               value);
//     EXPECT_EQ(value, expected);
// }
//
// // Disabled, because it requires a manually set up MQTT broker and a data sending MQTT client
// TEST_F(TCPSourceTest, DISABLED_testDeployOneWorkerWithTCPSourceConfig) {
//     CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
//     WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
//
//     coordinatorConfig->rpcPort = *rpcCoordinatorPort;
//     coordinatorConfig->restPort = *restPort;
//     wrkConf->coordinatorPort = *rpcCoordinatorPort;
//
//     NES_INFO("QueryDeploymentTest: Start coordinator");
//     NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
//     uint64_t port = crd->startCoordinator(/**blocking**/ false);
//     EXPECT_NE(port, 0UL);
//     //register logical source qnv
//     std::string source =
//         R"(Schema::create()->addField("type", DataTypeFactory::createArray(10, DataTypeFactory::createChar()))
//                             ->addField(createField("hospitalId", BasicType::UINT64))
//                             ->addField(createField("stationId", BasicType::UINT64))
//                             ->addField(createField("patientId", BasicType::UINT64))
//                             ->addField(createField("time", BasicType::UINT64))
//                             ->addField(createField("healthStatus", BasicType::UINT8))
//                             ->addField(createField("healthStatusDuration", BasicType::UINT32))
//                             ->addField(createField("recovered", BasicType::BOOLEAN))
//                             ->addField(createField("dead", BasicType::BOOLEAN));)";
//     crd->getSourceCatalogService()->registerLogicalSource("stream", source);
//     NES_INFO("QueryDeploymentTest: Coordinator started successfully");
//
//     NES_INFO("QueryDeploymentTest: Start worker 1");
//     wrkConf->coordinatorPort = port;
//     tcpSourceType->setUrl("ws://127.0.0.1:9002");
//     tcpSourceType->setClientId("testClients");
//     tcpSourceType->setUserName("testUser");
//     tcpSourceType->setTopic("demoCityHospital_1");
//     tcpSourceType->setQos(2);
//     tcpSourceType->setCleanSession(true);
//     tcpSourceType->setFlushIntervalMS(2000);
//     auto physicalSource = PhysicalSource::create("stream", "test_stream", tcpSourceType);
//     wrkConf->physicalSources.add(physicalSource);
//     NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
//     bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
//     EXPECT_TRUE(retStart1);
//     NES_INFO("QueryDeploymentTest: Worker1 started successfully");
//
//     QueryServicePtr queryService = crd->getQueryService();
//     QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
//
//     std::string outputFilePath = getTestResourceFolder() / "test.out";
//     NES_INFO("QueryDeploymentTest: Submit query");
//     string query = R"(Query::from("stream").filter(Attribute("hospitalId") < 5).sink(FileSinkDescriptor::create(")"
//         + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";
//     QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
//     GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
//     EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
//     sleep(2);
//     NES_INFO("QueryDeploymentTest: Remove query");
//     queryService->validateAndQueueStopRequest(queryId);
//     EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));
//
//     NES_INFO("QueryDeploymentTest: Stop worker 1");
//     bool retStopWrk1 = wrk1->stop(true);
//     EXPECT_TRUE(retStopWrk1);
//
//     NES_INFO("QueryDeploymentTest: Stop Coordinator");
//     bool retStopCord = crd->stopCoordinator(true);
//     EXPECT_TRUE(retStopCord);
//     NES_INFO("QueryDeploymentTest: Test finished");
// }
}// namespace NES
