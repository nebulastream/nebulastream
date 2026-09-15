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

#include <cstdint>
#include <string_view>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <nes-coordinator-bridge/coordinator.h>
#include <rust/cxx.h>
#include <BaseUnitTest.hpp>
#include <BridgeError.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

Bridge::StatementOutcome submit(const Bridge::Coordinator& coordinator, const std::string_view sql)
{
    return coordinator.submit(rust::Str{sql.data(), sql.size()}, Bridge::WaitMode::None, 0, false);
}

}

class CreateWorkerConfigTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("CreateWorkerConfigTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(CreateWorkerConfigTest, AnUnknownOptionFailsTheStatementAndWritesNoRow)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Embedded, rust::Str{});

    const auto rejected
        = submit(*coordinator, R"(CREATE WORKER 'localhost:18080' SET ('localhost:19090' AS DATA, '1' AS "worker"."no_such_option"))");
    EXPECT_EQ(rejected.error.code, static_cast<uint16_t>(ErrorCode::InvalidConfigParameter));

    const auto created = submit(*coordinator, R"(CREATE WORKER 'localhost:18080' SET ('localhost:19090' AS DATA))");
    EXPECT_TRUE(Bridge::isOk(created.error)) << std::string_view{created.error.msg.data(), created.error.msg.size()};
}

TEST_F(CreateWorkerConfigTest, ARemoteWorkerRejectsOptionsButNotADataAddress)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Remote, rust::Str{});

    const auto rejected = submit(
        *coordinator,
        R"(CREATE WORKER 'localhost:18080' SET ('localhost:19090' AS DATA, '4' AS "worker"."query_engine"."number_of_worker_threads"))");
    EXPECT_EQ(rejected.error.code, static_cast<uint16_t>(ErrorCode::InvalidConfigParameter));

    const auto created = submit(*coordinator, R"(CREATE WORKER 'localhost:18080' SET ('localhost:19090' AS DATA))");
    EXPECT_TRUE(Bridge::isOk(created.error)) << std::string_view{created.error.msg.data(), created.error.msg.size()};
}

TEST_F(CreateWorkerConfigTest, AValidOptionIsAccepted)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Embedded, rust::Str{});

    const auto created = submit(
        *coordinator,
        R"(CREATE WORKER 'localhost:18080' SET ('localhost:19090' AS DATA, '4' AS "worker"."query_engine"."number_of_worker_threads"))");
    EXPECT_TRUE(Bridge::isOk(created.error)) << std::string_view{created.error.msg.data(), created.error.msg.size()};
}

}
