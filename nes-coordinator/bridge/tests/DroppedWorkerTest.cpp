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

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>

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

void expectApplied(const Bridge::Coordinator& coordinator, const std::string_view sql)
{
    const auto outcome = submit(coordinator, sql);
    EXPECT_TRUE(Bridge::isOk(outcome.error)) << sql << ": " << std::string_view{outcome.error.msg.data(), outcome.error.msg.size()};
}

}

class DroppedWorkerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("DroppedWorkerTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(DroppedWorkerTest, ShowWorkersHidesADroppedWorker)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Remote, rust::Str{});
    expectApplied(*coordinator, "CREATE WORKER 'kept:8080' SET ('kept:9090' AS DATA)");
    expectApplied(*coordinator, "CREATE WORKER 'dropped:8080' SET ('dropped:9090' AS DATA)");
    expectApplied(*coordinator, "DROP WORKER WHERE HOST = 'dropped:8080'");

    const auto shown = submit(*coordinator, "SHOW WORKERS");

    ASSERT_TRUE(Bridge::isOk(shown.error));
    const std::string workers{shown.result};
    EXPECT_TRUE(workers.contains("kept:8080")) << workers;
    EXPECT_FALSE(workers.contains("dropped:8080")) << workers;
}

TEST_F(DroppedWorkerTest, AQueryWithASourceOnADroppedWorkerFailsPlacement)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Remote, rust::Str{});
    expectApplied(*coordinator, "CREATE WORKER 'kept:8080' SET ('kept:9090' AS DATA)");
    expectApplied(*coordinator, "CREATE WORKER 'dropped:8080' SET ('dropped:9090' AS DATA)");
    expectApplied(*coordinator, "CREATE LOGICAL SOURCE input (id UINT64 NOT NULL)");
    expectApplied(*coordinator, R"(CREATE PHYSICAL SOURCE FOR input TYPE File SET (
        'input.csv' AS "SOURCE".FILE_PATH,
        'dropped:8080' AS "SOURCE".HOST,
        'CSV' AS INPUT_FORMATTER."TYPE"))");
    expectApplied(*coordinator, R"(CREATE SINK output (id UINT64 NOT NULL) TYPE VOID SET ('kept:8080' AS "SINK"."HOST"))");
    expectApplied(*coordinator, "DROP WORKER WHERE HOST = 'dropped:8080'");

    const auto outcome = submit(*coordinator, "SELECT * FROM input INTO output");

    EXPECT_EQ(outcome.error.code, static_cast<uint16_t>(ErrorCode::PlacementFailure))
        << std::string_view{outcome.error.msg.data(), outcome.error.msg.size()};
}

TEST_F(DroppedWorkerTest, ADroppedAddressCanBeRegisteredAgain)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Remote, rust::Str{});
    expectApplied(*coordinator, "CREATE WORKER 'worker:8080' SET ('worker:9090' AS DATA)");
    expectApplied(*coordinator, "DROP WORKER WHERE HOST = 'worker:8080'");

    constexpr auto registerAgain = "CREATE WORKER 'worker:8080' SET ('worker:9091' AS DATA)";
    auto outcome = submit(*coordinator, registerAgain);
    /// Dropping a worker only marks it for removal, and the coordinator deletes its row afterwards.
    /// Until then the address is still taken, so the statement is retried.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{30};
    while (outcome.error.code == static_cast<uint16_t>(ErrorCode::WorkerAlreadyExists) and std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        outcome = submit(*coordinator, registerAgain);
    }

    ASSERT_TRUE(Bridge::isOk(outcome.error)) << std::string_view{outcome.error.msg.data(), outcome.error.msg.size()};
    const auto shown = submit(*coordinator, "SHOW WORKERS");
    ASSERT_TRUE(Bridge::isOk(shown.error));
    EXPECT_TRUE(std::string{shown.result}.contains("worker:9091")) << std::string{shown.result};
}

}
