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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestUtils.hpp>
#include <thread>

namespace NES {

class TCPSourceIntegrationTest : public Testing::NESBaseTest {
  public:
    static int sockfd;
    static sockaddr_in sockaddr;

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

    static void sendMessage(std::string message, int repeatSending) {
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
        }

        // Close the connections
        close(connection);
    }
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

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    auto sourceCatalog = crd->getSourceCatalog();

    struct Record {
        uint32_t id;
        float value;
        char name[8];
    };
    static_assert(sizeof(Record) == 16);

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("name", DataTypeFactory::createFixedChar(8));

    ASSERT_EQ(tcpSchema->getSchemaSizeInBytes(), sizeof(Record));

    sourceCatalog->addLogicalSource("tcpStream", tcpSchema);

    NES_INFO("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(9999);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(100);
    sourceConfig->setInputFormat(Configurations::InputFormat::CSV);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR);
    sourceConfig->setTupleSeparator('\n');

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = "tcpSourceTestOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    std::string message = "42, 5.893, hello\n";
    int repeatSending = 5;
    std::thread serverThread(sendMessage, message, repeatSending);
    serverThread.join();
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("MemorySourceIntegrationTest: Remove query");
    //ASSERT_TRUE(queryService->validateAndQueueStopQueryRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //    NES_INFO("MemorySourceIntegrationTest: content=" << content);
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

    ASSERT_EQ(5, lineCnt - 1);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

}// namespace NES