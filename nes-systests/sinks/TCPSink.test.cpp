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

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <expected>
#include <filesystem>
#include <fstream>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <BaseUnitTest.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <QuerySubmitter.hpp>
#include <SystestBinder.hpp>
#include <SystestConfiguration.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

extern void enable_memcom();

namespace
{
constexpr std::string_view QueryTemplate = R"(# name: sinks/TCPSink.test
# description: sending query output to a TCP sink
# groups: [Sinks, TCP]

CREATE SINK tcp_sink(id UINT64 NOT NULL, value UINT64 NOT NULL, timestamp UINT64 NOT NULL) TYPE TCP SET(
    'localhost:8080' AS `SINK`.`HOST`,
    '127.0.0.1' AS `SINK`.SOCKET_HOST,
    TCP_SINK_TEST_PORT AS `SINK`.SOCKET_PORT,
    'CSV' AS `SINK`.OUTPUT_FORMAT
);

SELECT ID, VALUE, TIMESTAMP
FROM File(
    'small/stream8.csv' AS `SOURCE`.FILE_PATH,
    'CSV' AS PARSER.`TYPE`,
    SCHEMA(id UINT64 NOT NULL, value UINT64 NOT NULL, timestamp UINT64 NOT NULL) AS `SOURCE`.`SCHEMA`)
INTO tcp_sink;
----
1,1,12
1,2,23
1,3,34
1,4,45
1,5,56
)";

std::filesystem::path createUniqueTempDirectory()
{
    static std::atomic_uint64_t counter = 0;
    const auto uniqueId = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path()
        / ("nes-systest-tcp-sink-" + std::to_string(uniqueId) + "-" + std::to_string(counter.fetch_add(1)));
    std::filesystem::create_directories(path);
    return path;
}

class TemporaryDirectory
{
public:
    TemporaryDirectory() : path(createUniqueTempDirectory()) { }

    ~TemporaryDirectory()
    {
        std::error_code errorCode;
        std::filesystem::remove_all(path, errorCode);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;
    TemporaryDirectory(TemporaryDirectory&&) = delete;
    TemporaryDirectory& operator=(TemporaryDirectory&&) = delete;

    [[nodiscard]] const std::filesystem::path& get() const { return path; }

private:
    std::filesystem::path path;
};

void writeTextFile(const std::filesystem::path& path, const std::string& content)
{
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    ASSERT_TRUE(out.is_open()) << "Failed to open file " << path;
    out << content;
    out.close();
}

std::string replacePortPlaceholder(std::string content, const uint16_t port)
{
    static constexpr std::string_view Token = "TCP_SINK_TEST_PORT";
    const auto pos = content.find(Token);
    EXPECT_NE(pos, std::string::npos);
    if (pos != std::string::npos)
    {
        content.replace(pos, Token.size(), std::to_string(port));
    }
    return content;
}

std::string dataTypeName(const NES::DataType::Type type)
{
    switch (type)
    {
        case NES::DataType::Type::UINT8:
            return "UINT8";
        case NES::DataType::Type::UINT16:
            return "UINT16";
        case NES::DataType::Type::UINT32:
            return "UINT32";
        case NES::DataType::Type::UINT64:
            return "UINT64";
        case NES::DataType::Type::INT8:
            return "INT8";
        case NES::DataType::Type::INT16:
            return "INT16";
        case NES::DataType::Type::INT32:
            return "INT32";
        case NES::DataType::Type::INT64:
            return "INT64";
        case NES::DataType::Type::FLOAT32:
            return "FLOAT32";
        case NES::DataType::Type::FLOAT64:
            return "FLOAT64";
        case NES::DataType::Type::BOOLEAN:
            return "BOOLEAN";
        case NES::DataType::Type::CHAR:
            return "CHAR";
        case NES::DataType::Type::UNDEFINED:
            return "UNDEFINED";
        case NES::DataType::Type::VARSIZED:
            return "VARSIZED";
    }
    std::unreachable();
}

std::string resultFileHeader(const NES::Schema& schema)
{
    std::string header;
    bool firstField = true;
    for (const auto& field : schema.getFields())
    {
        if (!firstField)
        {
            header += ",";
        }
        firstField = false;
        header += field.name;
        header += ":";
        header += dataTypeName(field.dataType.type);
        header += ":";
        header += field.dataType.nullable ? "IS_NULLABLE" : "NOT_NULLABLE";
    }
    return header;
}

class TcpListener
{
public:
    TcpListener()
    {
        listenerFd = socket(AF_INET, SOCK_STREAM, 0);
        if (listenerFd < 0)
        {
            throw std::runtime_error(fmt::format("Failed to create TCP listener socket: {}", std::strerror(errno)));
        }

        const int reuseAddr = 1;
        if (setsockopt(listenerFd, SOL_SOCKET, SO_REUSEADDR, &reuseAddr, sizeof(reuseAddr)) != 0)
        {
            throw std::runtime_error(fmt::format("Failed to configure SO_REUSEADDR: {}", std::strerror(errno)));
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = 0;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        if (bind(listenerFd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
        {
            throw std::runtime_error(fmt::format("Failed to bind TCP listener: {}", std::strerror(errno)));
        }
        if (listen(listenerFd, 1) != 0)
        {
            throw std::runtime_error(fmt::format("Failed to listen on TCP listener: {}", std::strerror(errno)));
        }

        socklen_t addressLength = sizeof(address);
        if (getsockname(listenerFd, reinterpret_cast<sockaddr*>(&address), &addressLength) != 0)
        {
            throw std::runtime_error(fmt::format("Failed to inspect TCP listener socket: {}", std::strerror(errno)));
        }
        port = ntohs(address.sin_port);

        payloadFuture = payloadPromise.get_future();
        listenerThread = std::jthread(
            [this]
            {
                try
                {
                    fd_set readSet;
                    FD_ZERO(&readSet);
                    FD_SET(listenerFd, &readSet);
                    timeval timeout{.tv_sec = 30, .tv_usec = 0};
                    const auto selectResult = select(listenerFd + 1, &readSet, nullptr, nullptr, &timeout);
                    if (selectResult <= 0)
                    {
                        payloadPromise.set_exception(
                            std::make_exception_ptr(std::runtime_error("Timed out waiting for TCP sink connection")));
                        return;
                    }

                    const int acceptedFd = accept(listenerFd, nullptr, nullptr);
                    if (acceptedFd < 0)
                    {
                        payloadPromise.set_exception(
                            std::make_exception_ptr(std::runtime_error(std::strerror(errno))));
                        return;
                    }

                    std::string payload;
                    std::array<char, 4096> buffer{};
                    while (true)
                    {
                        const auto readBytes = recv(acceptedFd, buffer.data(), buffer.size(), 0);
                        if (readBytes < 0)
                        {
                            close(acceptedFd);
                            payloadPromise.set_exception(
                                std::make_exception_ptr(std::runtime_error(std::strerror(errno))));
                            return;
                        }
                        if (readBytes == 0)
                        {
                            break;
                        }
                        payload.append(buffer.data(), static_cast<size_t>(readBytes));
                    }

                    close(acceptedFd);
                    payloadPromise.set_value(std::move(payload));
                }
                catch (...)
                {
                    payloadPromise.set_exception(std::current_exception());
                }
            });
    }

    ~TcpListener()
    {
        if (listenerFd >= 0)
        {
            close(listenerFd);
        }
    }

    [[nodiscard]] uint16_t getPort() const { return port; }

    std::string waitForPayload()
    {
        return payloadFuture.get();
    }

private:
    int listenerFd = -1;
    uint16_t port = 0;
    std::promise<std::string> payloadPromise;
    std::future<std::string> payloadFuture;
    std::jthread listenerThread;
};
}

namespace NES::Systest
{
class TCPSinkSystest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TCPSinkSystest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TCPSinkSystest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down TCPSinkSystest test class."); }
};

TEST_F(TCPSinkSystest, InlineSinkWritesCSVPayloadToTcpSocket)
{
    const TemporaryDirectory tempDir;
    TcpListener listener;

    const auto testFilePath = tempDir.get() / "TCPSink.test";
    writeTextFile(testFilePath, replacePortPlaceholder(std::string(QueryTemplate), listener.getPort()));

    SystestConfiguration config{};
    config.testsDiscoverDir.setValue(TEST_DISCOVER_DIR);
    config.directlySpecifiedTestFiles.setValue(testFilePath.string());
    config.workingDir.setValue((tempDir.get() / "working").string());
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = Host("localhost:8080"),
            .dataAddress = "localhost:9090",
            .maxOperators = Capacity(CapacityKind::Limited{1000}),
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {Host("localhost:8080")},
        .allowSinkPlacement = {Host("localhost:8080")}};

    std::filesystem::create_directories(config.workingDir.getValue());

    const auto discoveredTests = loadTestFileMap(config);
    ASSERT_EQ(discoveredTests.size(), 1U);

    SystestBinder binder{
        config.workingDir.getValue(),
        config.testDataDir.getValue(),
        config.configDir.getValue(),
        config.queryOptimizerConfig.value_or(QueryOptimizerConfiguration{}),
        config.clusterConfig};
    auto [queries, loadedFiles] = binder.loadOptimizeQueries(discoveredTests);
    ASSERT_EQ(loadedFiles, 1U);
    ASSERT_EQ(queries.size(), 1U);
    ASSERT_TRUE(queries.front().planInfoOrException.has_value()) << queries.front().planInfoOrException.error().what();

    enable_memcom();

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    for (const auto& [host, data, capacity, downstream, workerConfig] : config.clusterConfig.workers)
    {
        workerCatalog->addWorker(host, data, capacity, downstream, workerConfig);
    }

    QuerySubmitter submitter(
        std::make_unique<QueryManager>(std::move(workerCatalog), createEmbeddedBackend(SingleNodeWorkerConfiguration{})));
    const auto registration = submitter.registerQuery(queries.front().planInfoOrException.value().queryPlan);
    ASSERT_TRUE(registration.has_value()) << registration.error().what();

    submitter.startQuery(*registration);
    const auto queryStatus = submitter.waitForQueryTermination(*registration);
    ASSERT_EQ(queryStatus.getGlobalQueryStatus(), DistributedQueryStatus::Stopped);

    const auto payload = listener.waitForPayload();
    const auto header = resultFileHeader(queries.front().planInfoOrException.value().sinkOutputSchema);
    writeTextFile(queries.front().resultFile(), fmt::format("{}\n{}", header, payload));

    RunningQuery runningQuery{
        queries.front(),
        *registration,
        std::nullopt,
        queryStatus,
        0,
        0,
        false,
        std::nullopt,
    };
    const auto resultError = checkResult(runningQuery);
    ASSERT_FALSE(resultError.has_value()) << *resultError;
}
}
