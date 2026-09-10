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
///   GrpcSink   -- reports probe results to a StatisticReportService
///   GrpcSource -- turns RequestStatistic calls into rows that drive a probe
///
/// The statistic service is Rust and out of process, so the sink test stands a minimal StatisticReportService up
/// in-process and records what arrives.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <StatisticStore/DefaultStatisticStore.hpp>
#include <Util/Logger/Logger.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <StatisticReportRecorder.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticTestSupport.hpp>

namespace NES
{
using namespace StatisticTestSupport;

class StatisticTransportTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("StatisticTransportTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        globalStatisticStore().clear();
    }

    void TearDown() override
    {
        globalStatisticStore().clear();
        BaseUnitTest::TearDown();
    }
};

/// The whole write-to-statistic interface path: build, probe, and ship each result over gRPC. This is also what
/// establishes that reports carry a value at all -- the branch this is ported from never populated the field.
TEST_F(StatisticTransportTest, GrpcSinkReportsProbeResultsToTheStatisticService)
{
    TestStatisticServiceServer statisticInterface;
    ASSERT_TRUE(statisticInterface.isRunning()) << "could not start a test statisticInterface server";
    ASSERT_NE(statisticInterface.getPort(), 0U);

    const auto inputPath = writeInput("statistic-transport-input.csv");
    const auto plan = addGrpcSink(addStatisticStoreReader(buildStatisticPlan(inputPath)), statisticInterface.getPort());
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

    const auto sourcePort = pickFreePort();
    ASSERT_NE(sourcePort, 0U);
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    const Schema<UnqualifiedUnboundField, Ordered> requestSchema{
        UnqualifiedUnboundField{Identifier::parse("statisticId"), uint64Type},
        UnqualifiedUnboundField{Identifier::parse("startTs"), uint64Type},
        UnqualifiedUnboundField{Identifier::parse("endTs"), uint64Type}};

    auto plan = LogicalPlanBuilder::createLogicalPlan(
        Identifier::parse("Grpc"),
        requestSchema,
        {{Identifier::parse("grpc_port"), std::to_string(sourcePort)}, {Identifier::parse("host"), std::string{TEST_HOST.getRawValue()}}},
        {{Identifier::parse("type"), "CSV"}});
    plan = addFileSink(plan, outputPath);

    auto localPlan = optimizeToLocalPlan(plan);
    ASSERT_TRUE(localPlan.has_value());
    localPlan->setQueryId(QueryId{1});

    const SingleNodeWorkerConfiguration configuration;
    SingleNodeWorker worker{configuration};
    const auto queryId = worker.startQuery(localPlan.value());
    ASSERT_TRUE(queryId.has_value()) << queryId.error().what();

    /// The source binds during open(), which happens asynchronously after startQuery returns, so the first
    /// request may land before anything is listening. Retry until one is accepted.
    auto channel = grpc::CreateChannel("localhost:" + std::to_string(sourcePort), grpc::InsecureChannelCredentials());
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
