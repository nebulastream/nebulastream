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

/// Covers the gRPC transport in both directions, against real servers rather than mocks.
///
///   GrpcSink   -- reports probe results to a StatisticInterfaceService
///   GrpcSource -- turns RequestStatistic calls into rows that drive a probe
///
/// The statistic interface itself does not exist yet (that is the next phase), so the sink test stands a minimal
/// StatisticInterfaceService up in-process and records what arrives.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <StatisticStore/StatisticStoreRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>
#include <StatisticTestSupport.hpp>

namespace NES
{
namespace
{

using namespace StatisticTestSupport;

/// Stands in for the statistic interface until it is ported: records every report it receives.
class RecordingStatisticInterfaceService final : public StatisticInterfaceService::Service
{
public:
    grpc::Status ReportStatistic(grpc::ServerContext*, const StatisticReport* report, google::protobuf::Empty*) override
    {
        const std::lock_guard lock(mutex);
        reports.push_back(*report);
        return grpc::Status::OK;
    }

    [[nodiscard]] std::vector<StatisticReport> snapshot() const
    {
        const std::lock_guard lock(mutex);
        return reports;
    }

private:
    mutable std::mutex mutex;
    std::vector<StatisticReport> reports;
};

/// Owns a service and the server hosting it, on a kernel-chosen port.
class TestStatisticInterfaceServer
{
public:
    TestStatisticInterfaceServer()
    {
        grpc::ServerBuilder builder;
        int selected = 0;
        builder.AddListeningPort("0.0.0.0:0", grpc::InsecureServerCredentials(), &selected);
        builder.RegisterService(&service);
        server = builder.BuildAndStart();
        port = static_cast<uint32_t>(selected);
    }

    ~TestStatisticInterfaceServer()
    {
        if (server)
        {
            server->Shutdown();
        }
    }

    TestStatisticInterfaceServer(const TestStatisticInterfaceServer&) = delete;
    TestStatisticInterfaceServer& operator=(const TestStatisticInterfaceServer&) = delete;
    TestStatisticInterfaceServer(TestStatisticInterfaceServer&&) = delete;
    TestStatisticInterfaceServer& operator=(TestStatisticInterfaceServer&&) = delete;

    [[nodiscard]] uint32_t getPort() const { return port; }
    [[nodiscard]] bool isRunning() const { return server != nullptr; }
    [[nodiscard]] std::vector<StatisticReport> reports() const { return service.snapshot(); }

private:
    RecordingStatisticInterfaceService service;
    std::unique_ptr<grpc::Server> server;
    uint32_t port{0};
};

}

class StatisticTransportTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("StatisticTransportTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        StatisticStoreRegistry::instance().clear();
    }

    void TearDown() override
    {
        StatisticStoreRegistry::instance().clear();
        BaseUnitTest::TearDown();
    }
};

/// The whole write-to-statistic interface path: build, probe, and ship each result over gRPC. This is also what
/// establishes that reports carry a value at all -- the branch this is ported from never populated the field.
TEST_F(StatisticTransportTest, GrpcSinkReportsProbeResultsToTheStatisticInterface)
{
    TestStatisticInterfaceServer statisticInterface;
    ASSERT_TRUE(statisticInterface.isRunning()) << "could not start a test statisticInterface server";
    ASSERT_NE(statisticInterface.getPort(), 0U);

    const auto inputPath = writeInput("statistic-transport-input.csv");
    const auto plan = addGrpcSink(addScalarProbe(buildStatisticPlan(inputPath)), statisticInterface.getPort());
    runToCompletion(plan);

    /// The sink reports synchronously from execute(), so everything is in by the time the query stops.
    const auto reports = statisticInterface.reports();
    ASSERT_EQ(reports.size(), 2U) << "expected one report per closed window";

    std::vector<double> values;
    for (const auto& report : reports)
    {
        EXPECT_EQ(report.statistic_id(), STATISTIC_ID);
        EXPECT_EQ(report.end_ts() - report.start_ts(), WINDOW_SIZE_MS);
        values.push_back(report.value());
    }
    std::ranges::sort(values);
    EXPECT_DOUBLE_EQ(values.at(0), 20.0);
    EXPECT_DOUBLE_EQ(values.at(1), 200.0);
}

/// The impulse direction: a RequestStatistic call has to surface as a row the probe can act on. Verified
/// through a plain file sink, so a failure here is the source's and not the probe's.
TEST_F(StatisticTransportTest, GrpcSourceTurnsRequestsIntoRows)
{
    const auto outputPath = std::filesystem::temp_directory_path() / "statistic-transport-source-output.csv";
    std::filesystem::remove(outputPath);

    /// A fixed port, because the request has to be addressed before the source reports what it bound.
    constexpr uint32_t SOURCE_PORT = 41401;
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    const Schema<UnqualifiedUnboundField, Ordered> requestSchema{
        UnqualifiedUnboundField{Identifier::parse("statisticId"), uint64Type},
        UnqualifiedUnboundField{Identifier::parse("startTs"), uint64Type},
        UnqualifiedUnboundField{Identifier::parse("endTs"), uint64Type}};

    auto plan = LogicalPlanBuilder::createLogicalPlan(
        Identifier::parse("Grpc"),
        requestSchema,
        {{Identifier::parse("grpc_port"), std::to_string(SOURCE_PORT)},
         {Identifier::parse("host"), std::string{TEST_HOST.getRawValue()}}},
        {{Identifier::parse("type"), "CSV"}});
    plan = addFileSink(plan, outputPath);

    const auto localPlan = optimizeToLocalPlan(plan);
    ASSERT_TRUE(localPlan.has_value());

    const SingleNodeWorkerConfiguration configuration;
    SingleNodeWorker worker{configuration};
    const auto queryId = worker.startQuery(localPlan.value());
    ASSERT_TRUE(queryId.has_value()) << queryId.error().what();

    /// The source binds during open(), which happens asynchronously after startQuery returns, so the first
    /// request may land before anything is listening. Retry until one is accepted.
    auto channel = grpc::CreateChannel("localhost:" + std::to_string(SOURCE_PORT), grpc::InsecureChannelCredentials());
    auto stub = StatisticSourceService::NewStub(channel);
    bool delivered = false;
    for (int attempt = 0; attempt < 100 and not delivered; ++attempt)
    {
        StatisticRequest request;
        request.set_statistic_id(STATISTIC_ID);
        request.set_start_ts(0);
        request.set_end_ts(WINDOW_SIZE_MS);

        grpc::ClientContext context;
        google::protobuf::Empty response;
        delivered = stub->RequestStatistic(&context, request, &response).ok();
        if (not delivered)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        }
    }
    ASSERT_TRUE(delivered) << "the source never accepted a RequestStatistic call";

    /// The source never reaches end-of-stream, so the query has to be stopped rather than waited out.
    for (int attempt = 0; attempt < 100; ++attempt)
    {
        if (std::filesystem::exists(outputPath) and readFile(outputPath).find("401,0,1000") != std::string::npos)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    const auto stopped = worker.stopQuery(queryId.value());
    EXPECT_TRUE(stopped.has_value()) << "stopping the query failed";

    ASSERT_TRUE(std::filesystem::exists(outputPath)) << "the source produced no output";
    const auto output = readFile(outputPath);
    EXPECT_NE(output.find("401,0,1000"), std::string::npos) << "the requested probe did not appear as a row in:\n" << output;
}

}
