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

#include <string>
#include <Identifiers/Identifiers.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <WorkerCatalogEntry.hpp>

namespace NES::Test
{
namespace
{
/// A worker built before the RequestVersion RPC existed: the generated service base answers every RPC
/// that is not overridden with UNIMPLEMENTED, which is exactly what such a worker puts on the wire.
class OldWorkerService final : public WorkerRPCService::Service
{
};

/// Only `host` matters here; everything else stays default so the ctor's isExplicitlySet() warning stays quiet.
GRPCQuerySubmissionBackend backendFor(const std::string& host)
{
    return GRPCQuerySubmissionBackend{
        WorkerCatalogEntry{.host = Host{host}, .dataAddress = {}, .maxOperators = {}, .downstream = {}, .config = {}}};
}
}

class GRPCQuerySubmissionBackendTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("GRPCQuerySubmissionBackend.log", LogLevel::LOG_DEBUG); }
};

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
