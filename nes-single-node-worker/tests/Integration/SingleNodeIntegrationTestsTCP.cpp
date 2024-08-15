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
#include <TestUtils/UtilityFunctions.hpp>
#include <detail/PortDispatcher.hpp>
#include <fmt/core.h>
#include <BaseIntegrationTest.hpp>
#include <BorrowedPort.hpp>
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
    int numSources;
    int expectedNumTuples;
    int expectedCheckSum;

    /// Add this method to your QueryTestParam struct
    friend std::ostream& operator<<(std::ostream& os, const QueryTestParam& param)
    {
        return os << "QueryTestParam{queryFile: \"" << param.queryFile << "\", expectedTuples: " << param.expectedNumTuples << "}";
    }
};

/**
 * @brief Integration tests for the SingleNodeWorker. Tests entire code path from registration to running the query, stopping and
 * unregistration.
 */
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
};

class SyncedMockTcpServer
{
private:
    using tcp = boost::asio::ip::tcp;
    constexpr static int NUM_TUPLES_TO_PRODUCE = 200;

public:
    SyncedMockTcpServer(const short port) : acceptor(io_context, tcp::endpoint(tcp::v4(), port)) { }

    static std::unique_ptr<SyncedMockTcpServer> create(uint16_t port) { return std::make_unique<SyncedMockTcpServer>(port); }

    void run()
    {
        tcp::socket socket(io_context);
        acceptor.accept(socket);

        for (uint32_t i = 0; i < NUM_TUPLES_TO_PRODUCE; ++i)
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
};

TEST_P(SingleNodeIntegrationTest, TestQueryRegistration)
{
    using ResultSchema = struct
    {
        uint64_t id;
    };

    const auto& [queryName, numSources, expectedNumTuples, expectedCheckSum] = GetParam();
    const std::string queryInputFile = fmt::format("{}.bin", queryName);
    const std::string queryResultFile = fmt::format("{}.csv", queryName);
    IntegrationTestUtil::removeFile(queryResultFile); /// remove outputFile if exists

    SerializableDecomposedQueryPlan queryPlan;
    if (!IntegrationTestUtil::loadFile(queryPlan, queryInputFile))
    {
        GTEST_SKIP();
    }
    IntegrationTestUtil::replaceFileSinkPath(queryPlan, fmt::format("{}.csv", queryName));

    Configuration::SingleNodeWorkerConfiguration configuration{};
    configuration.queryCompilerConfiguration.nautilusBackend = QueryCompilation::NautilusBackend::MLIR_COMPILER_BACKEND;

    GRPCServer uut{SingleNodeWorker{configuration}};

    /// For every tcp source, get a free port, replace the port of one tcp source with the free port and create a server with that port.
    std::vector<std::unique_ptr<SyncedMockTcpServer>> mockedTcpServers;
    for (auto tcpSourceNumber = 0; tcpSourceNumber < numSources; ++tcpSourceNumber)
    {
        auto mockTcpServerPort = static_cast<uint16_t>(*detail::getPortDispatcher().getNextPort());
        IntegrationTestUtil::replacePortInTcpSources(queryPlan, mockTcpServerPort, tcpSourceNumber);
        mockedTcpServers.emplace_back(SyncedMockTcpServer::create(mockTcpServerPort));
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

    /// Todo (#166) : stop query might be called to early, leading to no received data.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    IntegrationTestUtil::stopQuery(queryId, HardStop, uut);
    IntegrationTestUtil::unregisterQuery(queryId, uut);

    Runtime::BufferManagerPtr bufferManager = Runtime::BufferManager::create();
    const auto sinkSchema = IntegrationTestUtil::loadSinkSchema(queryPlan);
    auto buffers = Runtime::Execution::Util::createBuffersFromCSVFile(queryResultFile, sinkSchema, *bufferManager, 0, "", true);

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

    IntegrationTestUtil::removeFile(queryResultFile);
}

INSTANTIATE_TEST_CASE_P(
    QueryTests,
    SingleNodeIntegrationTest,
    testing::Values(
        QueryTestParam{"qOneTCPSource", 1, 200, 19900 /* SUM(0, 1, ..., 199) */},
        QueryTestParam{"qOneTCPSourceWithFilter", 1, 16, 120 /* SUM(0, 1, ..., 31) */},
        QueryTestParam{"qTwoTCPSourcesWithFilter", 2, 32, 240 /* 2*SUM(0, 1, ..., 31) */}));
} /// namespace NES::Testing