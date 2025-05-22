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

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <Nautilus/NautilusBackend.hpp>
#include <Runtime/BufferManager.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <GrpcService.hpp>
#include <IntegrationTestUtil.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

#include <boost/asio.hpp>

namespace NES::Testing
{
using namespace ::testing;

struct QueryTestParam
{
    std::string queryFile;
    int numSourcesTCP;
    int expectedNumTuples;
    int expectedCheckSum;
    size_t numInputTuplesToProduceByTCPMockServer;

    /// Add this method to your QueryTestParam struct
    friend std::ostream& operator<<(std::ostream& os, const QueryTestParam& param)
    {
        return os << "QueryTestParam{queryFile: \"" << param.queryFile << "\", expectedTuples: " << param.expectedNumTuples << "}";
    }
};

class SingleNodeIntegrationTest : public BaseIntegrationTest, public testing::WithParamInterface<QueryTestParam>
{
public:
    static void SetUpTestCase()
    {
        BaseIntegrationTest::SetUpTestCase();
        Logger::setupLogging("SingleNodeIntegrationTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SingleNodeIntegrationTest test class.");
    }

    const std::string idFieldName = "tcp_source$id";
    const std::string dataInputFile = "oneToThirtyOneSingleColumn.csv";
};

class SyncedMockTcpServer
{
private:
    using tcp = boost::asio::ip::tcp;

public:
    SyncedMockTcpServer(size_t numInputTuplesToProduce)
        : acceptor(io_context, tcp::endpoint(tcp::v4(), 0)), numInputTuplesToProduce(numInputTuplesToProduce)
    {
    }

    static std::unique_ptr<SyncedMockTcpServer> create(size_t numInputTuplesToProduce)
    {
        return std::make_unique<SyncedMockTcpServer>(numInputTuplesToProduce);
    }

    uint16_t getPort() const { return acceptor.local_endpoint().port(); }

    void run()
    {
        tcp::socket socket(io_context);
        acceptor.accept(socket);

        for (uint32_t i = 0; i < numInputTuplesToProduce; ++i)
        {
            std::string data = std::to_string(i) + "\n";
            boost::asio::write(socket, boost::asio::buffer(data));
        }

        boost::system::error_code errorCode;
        socket.shutdown(tcp::socket::shutdown_both, errorCode);
        socket.close(errorCode);
    }

private:
    boost::asio::io_context io_context;
    tcp::acceptor acceptor;
    size_t numInputTuplesToProduce;
};

TEST_P(SingleNodeIntegrationTest, IntegrationTestWithSourcesMixed)
{
    using ResultSchema = struct
    {
        uint64_t id;
    };

    const auto& [queryName, numSourcesTCP, expectedNumTuples, expectedCheckSum, numInputTuplesToProduceByTCPMockServer] = GetParam();
    const auto testSpecificIdentifier = IntegrationTestUtil::getUniqueTestIdentifier();
    const auto testSpecificResultFileName = fmt::format("{}.csv", testSpecificIdentifier);
    const auto testSpecificDataFileName = fmt::format("{}_{}", testSpecificIdentifier, dataInputFile);

    const std::string queryInputFile = fmt::format("{}.bin", queryName);
    IntegrationTestUtil::removeFile(testSpecificResultFileName); /// remove outputFile if exists

    SerializableDecomposedQueryPlan queryPlan;
    if (!IntegrationTestUtil::loadFile(queryPlan, queryInputFile, dataInputFile, testSpecificDataFileName))
    {
        GTEST_SKIP();
    }
    IntegrationTestUtil::replaceFileSinkPath(queryPlan, testSpecificResultFileName);
    IntegrationTestUtil::replaceInputFileInFileSources(queryPlan, testSpecificDataFileName);

    Configuration::SingleNodeWorkerConfiguration configuration{};
    configuration.workerConfiguration.queryCompiler.nautilusBackend = Nautilus::Configurations::NautilusBackend::COMPILER;

    GRPCServer uut{SingleNodeWorker{configuration}};

    /// For every tcp source, get a free port, replace the port of one tcp source with the free port and create a server with that port.
    std::vector<std::unique_ptr<SyncedMockTcpServer>> mockedTcpServers;
    for (auto tcpSourceNumber = 0; tcpSourceNumber < numSourcesTCP; ++tcpSourceNumber)
    {
        auto mockTCPServer = SyncedMockTcpServer::create(numInputTuplesToProduceByTCPMockServer);
        auto mockTCPServerPort = mockTCPServer->getPort();
        IntegrationTestUtil::replacePortInTCPSources(queryPlan, mockTCPServerPort, tcpSourceNumber);
        mockedTcpServers.emplace_back(std::move(mockTCPServer));
    }

    /// Register the query and start it.
    auto queryId = IntegrationTestUtil::registerQueryPlan(queryPlan, uut);
    IntegrationTestUtil::startQuery(queryId, uut);

    /// Start all SyncedMockTcpServers and wait until every sever sent all tuples.
    for (const auto& server : mockedTcpServers)
    {
        std::thread serverThread([&server]() { server->run(); });
        serverThread.join(); /// wait for serverThread to finish
    }

    ASSERT_TRUE(IntegrationTestUtil::waitForQueryToEnd(queryId, uut));
    IntegrationTestUtil::unregisterQuery(queryId, uut);

    auto bufferManager = Memory::BufferManager::create();
    const auto sinkSchema = IntegrationTestUtil::loadSinkSchema(queryPlan);
    auto buffers = IntegrationTestUtil::createBuffersFromCSVFile(testSpecificResultFileName, sinkSchema, *bufferManager, 0, "", true);

    size_t numProcessedTuples = 0;
    size_t checkSum = 0; /// simple summation of all values
    for (const auto& buffer : buffers)
    {
        numProcessedTuples += buffer.getNumberOfTuples();
        for (size_t i = 0; i < buffer.getNumberOfTuples(); ++i)
        {
            checkSum += buffer.getBuffer<ResultSchema>()[i].id;
        }
    }
    EXPECT_EQ(numProcessedTuples, expectedNumTuples) << "Query did not produce the expected number of tuples";
    EXPECT_EQ(checkSum, expectedCheckSum) << "Query did not produce the expected expected checksum";

    IntegrationTestUtil::removeFile(testSpecificResultFileName);
    IntegrationTestUtil::removeFile(testSpecificDataFileName);
}

INSTANTIATE_TEST_CASE_P(
    QueryTests,
    SingleNodeIntegrationTest,
    testing::Values(QueryTestParam{"qOneCSVSourceAndOneTCPSourceWithFilter", 1, 62, 960 /* 2*SUM(0, 1, ..., 15) */, 32}));
}
