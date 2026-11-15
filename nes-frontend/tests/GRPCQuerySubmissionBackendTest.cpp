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

#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <WorkerConfig.hpp>

namespace NES::Test
{
namespace
{
/// A worker built before the RequestVersion RPC existed: the generated service base answers every RPC
/// that is not overridden with UNIMPLEMENTED, which is exactly what such a worker puts on the wire.
class OldWorkerService final : public WorkerRPCService::Service
{
};

class BatchWorkerService final : public WorkerRPCService::Service
{
public:
    std::atomic<int> requests{0};
    std::atomic<int> plans{0};
    std::vector<LocalQueryId> replyIds{
        LocalQueryId{std::string{"00000000-0000-4000-8000-000000000101"}},
        LocalQueryId{std::string{"00000000-0000-4000-8000-000000000202"}}};
    bool fail = false;

    grpc::Status StartQueries(grpc::ServerContext*, const StartQueriesRequest* request, StartQueriesReply* reply) override
    {
        ++requests;
        plans.fetch_add(request->queryplans_size());
        if (fail)
        {
            return {grpc::StatusCode::INTERNAL, "batch rejected by test worker"};
        }
        for (const auto id : replyIds)
        {
            *reply->add_queryids() = QueryPlanSerializationUtil::serializeQueryId(QueryId::createLocal(id));
        }
        return grpc::Status::OK;
    }
};

/// Shutdown also runs when a fatal assertion exits a test early.
class InProcessWorker
{
public:
    explicit InProcessWorker(WorkerRPCService::Service& service)
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        server = builder.BuildAndStart();
    }

    ~InProcessWorker()
    {
        if (server)
        {
            server->Shutdown();
            server->Wait();
        }
    }

    int port = 0;
    std::unique_ptr<grpc::Server> server;
};

std::vector<LogicalPlan> batchPlans()
{
    const Schema<UnqualifiedUnboundField, Ordered> schema{UnqualifiedUnboundField{Identifier::parse("VALUE"), DataType::Type::UINT64}};
    SourceCatalog sources;
    SinkCatalog sinks;
    const Host host{"localhost:8080"};
    const auto source = sources.getAnonymousSource(
        Identifier::parse("FILE"), schema, host, {{Identifier::parse("TYPE"), "NATIVE"}}, {{Identifier::parse("FILE_PATH"), "unused.csv"}});
    const auto sink = sinks.getAnonymousSink(schema, Identifier::parse("VOID"), host, {}, {});
    if (!source || !sink)
    {
        throw TestException("Could not construct serializable batch-test descriptors");
    }
    const LogicalOperator root = SinkLogicalOperator::create(SourceDescriptorLogicalOperator::create(*source), *sink);
    std::vector<LogicalPlan> plans;
    for (const auto& id : {DistributedQueryId{"first"}, DistributedQueryId{"second"}})
    {
        plans.emplace_back(QueryId::createDistributed(id), std::vector<LogicalOperator>{root});
    }
    return plans;
}

/// Only `host` matters here; everything else stays default so the ctor's isExplicitlySet() warning stays quiet.
GRPCQuerySubmissionBackend backendFor(const std::string& host)
{
    return GRPCQuerySubmissionBackend{
        WorkerConfig{.host = Host{host}, .dataAddress = {}, .maxOperators = {}, .downstream = {}, .config = {}}};
}
}

class GRPCQuerySubmissionBackendTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("GRPCQuerySubmissionBackend.log", LogLevel::LOG_DEBUG); }
};

TEST_F(GRPCQuerySubmissionBackendTest, StartQueriesUsesOneBatchRPCAndPreservesDistributedIds)
{
    BatchWorkerService service;
    InProcessWorker worker{service};
    ASSERT_NE(worker.server, nullptr);
    auto backend = backendFor(fmt::format("127.0.0.1:{}", worker.port));

    const auto result = backend.startQueries(batchPlans());

    EXPECT_EQ(service.requests.load(), 1);
    EXPECT_EQ(service.plans.load(), 2);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 2);
    EXPECT_EQ(result->at(0).getLocalQueryId(), service.replyIds[0]);
    EXPECT_EQ(result->at(0).getDistributedQueryId(), DistributedQueryId{"first"});
    EXPECT_EQ(result->at(1).getLocalQueryId(), service.replyIds[1]);
    EXPECT_EQ(result->at(1).getDistributedQueryId(), DistributedQueryId{"second"});
}

TEST_F(GRPCQuerySubmissionBackendTest, StartQueriesWorkerErrorReturnsQueryStartFailed)
{
    BatchWorkerService service;
    service.fail = true;
    InProcessWorker worker{service};
    ASSERT_NE(worker.server, nullptr);
    auto backend = backendFor(fmt::format("127.0.0.1:{}", worker.port));

    EXPECT_NO_THROW({
        const auto result = backend.startQueries(batchPlans());
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), ErrorCode::QueryStartFailed);
    });
    EXPECT_EQ(service.requests.load(), 1);
    EXPECT_EQ(service.plans.load(), 2);
}

TEST_F(GRPCQuerySubmissionBackendTest, StartQueriesMismatchedReplyCountReturnsErrorWithoutThrowing)
{
    /// Both a truncated reply and an oversized reply must be rejected before indexing the plans.
    for (const int replyCount : {1, 3})
    {
        SCOPED_TRACE(replyCount);
        BatchWorkerService service;
        service.replyIds.assign(replyCount, LocalQueryId{std::string{"00000000-0000-4000-8000-000000000101"}});
        InProcessWorker worker{service};
        ASSERT_NE(worker.server, nullptr);
        auto backend = backendFor(fmt::format("127.0.0.1:{}", worker.port));

        EXPECT_NO_THROW({
            const auto result = backend.startQueries(batchPlans());
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), ErrorCode::QueryStartFailed);
        });
        EXPECT_EQ(service.requests.load(), 1);
        EXPECT_EQ(service.plans.load(), 2);
    }
}

TEST_F(GRPCQuerySubmissionBackendTest, VersionOnOldWorkerReportsNotImplemented)
{
    OldWorkerService service;
    int port = 0;
    grpc::ServerBuilder builder;
    /// port 0 lets the kernel pick a free port and write it back, so parallel test runs cannot collide
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    const auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    const auto backend = backendFor(fmt::format("127.0.0.1:{}", port));
    const auto result = backend.version();

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::NotImplemented);

    server->Shutdown();
    server->Wait();
}

/// Pins that only UNIMPLEMENTED maps to NotImplemented; any other gRPC failure must not claim the worker
/// is too old.
TEST_F(GRPCQuerySubmissionBackendTest, VersionOnUnreachableWorkerIsNotNotImplemented)
{
    const auto backend = backendFor("127.0.0.1:1");
    const auto result = backend.version();

    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().code(), ErrorCode::NotImplemented);
}
}
