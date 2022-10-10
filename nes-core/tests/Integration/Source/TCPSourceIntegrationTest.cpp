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
#include <cstdlib>// For exit() and EXIT_FAILURE
#include <gtest/gtest.h>
#include <iostream>    // For cout
#include <netinet/in.h>// For sockaddr_in
#include <sys/socket.h>// For socket functions
#include <unistd.h>    // For read

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/QueryService.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/TCPSource.hpp>
#include <Util/TestHarness/TestHarness.hpp>

#include <Util/TestUtils.hpp>
#include <thread>

namespace NES {

class TCPSourceIntegrationTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TCPSourceIntegrationTest.log", NES::LogLevel::LOG_TRACE);
        NES_INFO("Setup TCPSourceIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        startServer();
    }

    void TearDown() override {
        Testing::NESBaseTest::TearDown();
        NES_INFO("Tear down TCPSourceIntegrationTest class.");
        stopServer();
    }

    void startServer() {
        // Create a socket (IPv4, TCP)
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            NES_ERROR("Failed to create socket. errno: " << errno);
            exit(EXIT_FAILURE);
        }

        // Listen to port 9999 on any address
        sockaddr.sin_family = AF_INET;
        sockaddr.sin_addr.s_addr = INADDR_ANY;
        sockaddr.sin_port = htons(9999);// htons is necessary to convert a number to
                                        // network byte order
        if (bind(sockfd, (struct sockaddr*) &sockaddr, sizeof(sockaddr)) < 0) {
            NES_ERROR("Failed to bind to port 9999. errno: " << errno);
            exit(EXIT_FAILURE);
        }

        // Start listening. Hold at most 10 connections in the queue
        if (listen(sockfd, 10) < 0) {
            NES_ERROR("Failed to listen on socket. errno: " << errno);
            exit(EXIT_FAILURE);
        }
    }

    void stopServer() {
        // Close the connections
        close(sockfd);
    }

    static void sendMessageString(std::string message, int repeatSending, int sleep = 1) {
        // Grab a connection from the queue
        auto addrlen = sizeof(sockaddr);

        int connection = accept(sockfd, (struct sockaddr*) &sockaddr, (socklen_t*) &addrlen);
        if (connection < 0) {
            NES_ERROR("Failed to grab connection. errno: " << errno);
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < repeatSending; ++i) {
            NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message << " iter=" << i);
            send(connection, message.c_str(), message.size(), 0);
            std::this_thread::sleep_for(std::chrono::seconds(sleep));
        }

        // Close the connections
        close(connection);
    }

    /*static void sendMessageJSON(web::json::value message, int repeatSending, int sleep = 1) {
        // Grab a connection from the queue
        auto addrlen = sizeof(sockaddr);

        int connection = accept(sockfd, (struct sockaddr*) &sockaddr, (socklen_t*) &addrlen);
        if (connection < 0) {
            NES_ERROR("Failed to grab connection. errno: " << errno);
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < repeatSending; ++i) {
            std::string messageAsString;
            try {
                messageAsString = message;
            } catch (std::exception e) {
                NES_ERROR("TCPSourceIntegrationTest::sendMessageJSON: Error " << strerror(errno));
            }
            NES_TRACE("TCPSourceIntegrationTest: Sending message: " << messageAsString << " iter=" << i);
            send(connection, messageAsString.c_str(), messageAsString.size(), 0);
            std::this_thread::sleep_for(std::chrono::seconds(sleep));
        }

        // Close the connections
        close(connection);
    }*/

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    static int sockfd;
    static sockaddr_in sockaddr;
};

int TCPSourceIntegrationTest::sockfd = 0;
sockaddr_in TCPSourceIntegrationTest::sockaddr = {};

TEST_F(TCPSourceIntegrationTest, TCPSourceReadCSVDataWithSeparatorToken) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("onTime", BOOLEAN);

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(9999);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(0);
    sourceConfig->setInputFormat(Configurations::InputFormat::CSV);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR);
    sourceConfig->setTupleSeparator('\n');

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 30;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString =
        R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")"
        + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    std::string message = "42,5.893,true\n";
    int repeatSending = 6;
    std::thread serverThread(sendMessageString, message, repeatSending, 1);
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$onTime:BOOLEAN|\n"
                             "+----------------------------------------------------+\n"
                             "|42|5.893000|1|\n"
                             "|42|5.893000|1|\n"
                             "|42|5.893000|1|\n"
                             "+----------------------------------------------------++----------------------------------------------------+\n"
                             "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$onTime:BOOLEAN|\n"
                             "+----------------------------------------------------+\n"
                             "|42|5.893000|1|\n"
                             "|42|5.893000|1|\n"
                             "|42|5.893000|1|\n"
                             "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    /*bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);*/
}

TEST_F(TCPSourceIntegrationTest, TCPSourceReadJSONDataWithSeparatorToken) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("name", DataTypeFactory::createFixedChar(5));

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(9999);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(0);
    sourceConfig->setInputFormat(Configurations::InputFormat::JSON);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR);
    sourceConfig->setTupleSeparator('\n');

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 50;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString =
        R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")"
        + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    /*web::json::value message{};
    message["id"] = web::json::value::string("42");
    message["value"] = web::json::value::string("5.893");
    message["name"] = web::json::value::string("hello");*/
    std::string message = "{\"id\":\"42\", \"value\":\"5.893\", \"name\":\"hello\"}";
    int repeatSending = 6;
    std::thread serverThread(sendMessageString, message, repeatSending, 1);
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
                             "+----------------------------------------------------+\n"
                             "|42|5.893000|hello|\n"
                             "|42|5.893000|hello|\n"
                             "|42|5.893000|hello|\n"
                             "+----------------------------------------------------++----------------------------------------------------+\n"
                             "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
                             "+----------------------------------------------------+\n"
                             "|42|5.893000|hello|\n"
                             "|42|5.893000|hello|\n"
                             "|42|5.893000|hello|\n"
                             "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    /*bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);*/
}

//todo: adapt code
TEST_F(TCPSourceIntegrationTest, TCPSourceReadCSVDataWithVariableMessageLength) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("onTime", BOOLEAN);

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(9999);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(0);
    sourceConfig->setInputFormat(Configurations::InputFormat::CSV);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET);
    sourceConfig->setBytesUsedForSocketBufferSizeTransfer(2);

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 30;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString =
        R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")"
        + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    std::string message = "42,5.893,true\n";
    int repeatSending = 6;
    std::thread serverThread(sendMessageString, message, repeatSending, 1);
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$onTime:BOOLEAN|\n"
                             "+----------------------------------------------------+\n"
                             "|42|5.893000|1|\n"
                             "|42|5.893000|1|\n"
                             "|42|5.893000|1|\n"
                             "+----------------------------------------------------++----------------------------------------------------+\n"
                             "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$onTime:BOOLEAN|\n"
                             "+----------------------------------------------------+\n"
                             "|42|5.893000|1|\n"
                             "|42|5.893000|1|\n"
                             "|42|5.893000|1|\n"
                             "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    /*bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);*/
}

}// namespace NES