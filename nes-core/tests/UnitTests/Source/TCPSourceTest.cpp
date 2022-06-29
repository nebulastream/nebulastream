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

#include "Common/DataTypes/DataTypeFactory.hpp"
#include "Util/TestUtils.hpp"
#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/TCPSourceType.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#define OPERATORID 1

#define ORIGINID 1

#define NUMSOURCELOCALBUFFERS 12

#define SUCCESSORS                                                                                                               \
    {}

#define MAX 80
#define PORT 9999
#define SA struct sockaddr

namespace NES {

class TCPSourceTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TCPSourceTest.log", NES::LogLevel::LOG_TRACE);
        NES_DEBUG("TCPSOURCETEST::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_DEBUG("TCPSOURCETEST::SetUp() MQTTSourceTest cases set up.");
        test_schema = Schema::create()->addField("var", UINT32);
        tcpSourceType = TCPSourceType::create();
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
 * Tests basic set up of MQTT source
 */
TEST_F(TCPSourceTest, TCPSourceInit) {

    auto tcpSource = createTCPSource(test_schema,
                                     bufferManager,
                                     queryManager,
                                     tcpSourceType,
                                     OPERATORID,
                                     ORIGINID,
                                     NUMSOURCELOCALBUFFERS,
                                     SUCCESSORS);

    SUCCEED();
}

/**
 * Test if schema, MQTT server address, clientId, user, and topic are the same
 */
TEST_F(TCPSourceTest, TCPSourcePrint) {
    tcpSourceType->setSocketHost("127.0.0.1");
    tcpSourceType->setSocketPort(5000);
    tcpSourceType->setSocketDomainViaString("AF_INET");
    tcpSourceType->setSocketTypeViaString("SOCK_STREAM");

    auto tcpSource = createTCPSource(test_schema,
                                     bufferManager,
                                     queryManager,
                                     tcpSourceType,
                                     OPERATORID,
                                     ORIGINID,
                                     NUMSOURCELOCALBUFFERS,
                                     SUCCESSORS);

    std::string expected = "TCPSOURCE(SCHEMA(var:INTEGER ), TCPSourceType => {\nsocketHost: 127.0.0.1\nsocketPort: "
                           "5000\nsocketDomain: 2\nsocketType: 1\ninputFormat: 1\n}";

    EXPECT_EQ(tcpSource->toString(), expected);

    SUCCEED();
}

/**
 * Tests if obtained value is valid.
 */
TEST_F(TCPSourceTest, TCPSourceConnectWithClient) {

    tcpSourceType->setSocketPort(9999);
    tcpSourceType->setSocketDomainViaString("AF_INET");
    tcpSourceType->setSocketTypeViaString("SOCK_STREAM");
    tcpSourceType->setInputFormat(Configurations::InputFormat::CSV);

    // After chatting close the socket
    auto schema = Schema::create()
                           ->addField("id", UINT32)
                           ->addField("value", FLOAT32)
                           ->addField("name", DataTypeFactory::createFixedChar(8));
    auto tcpSource = createTCPSource(schema,
                                     bufferManager,
                                     queryManager,
                                     tcpSourceType,
                                     OPERATORID,
                                     ORIGINID,
                                     NUMSOURCELOCALBUFFERS,
                                     SUCCESSORS);

    auto tuple_buffer = tcpSource->receiveData();

    /*EXPECT_TRUE(tuple_buffer.has_value());
    std::cout << tuple_buffer->getBuffer() << std::endl;

    struct Output {
        uint32_t id;
        float value;
        std::array<char, 8> name;
        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (id == rhs.id && value == rhs.value && name == rhs.name);
        }
    };

    std::vector<Output>* obtained = reinterpret_cast<std::vector<Output>*>(tuple_buffer->getBuffer());

    std::vector<Output> expected = {{42, 0.5, {"hello"}}};

    EXPECT_THAT(expected.data(), obtained->data());*/

}// namespace NES

// Disabled, because it requires a manually set up MQTT broker and a data sending MQTT client
TEST_F(TCPSourceTest, testDeployOneWorkerWithTCPSourceConfig) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("QueryDeploymentTest: Start coordinator");
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
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    wrkConf->coordinatorPort = port;
    tcpSourceType->setSocketHost("127.0.0.1");
    tcpSourceType->setSocketPort(5000);
    tcpSourceType->setSocketDomainViaString("AF_INET");
    tcpSourceType->setSocketTypeViaString("SOCK_STREAM");
    //auto physicalSource = PhysicalSource::create("stream", "test_stream", tcpSourceType);
    //wrkConf->physicalSources.add(physicalSource);
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
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
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
