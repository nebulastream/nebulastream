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

#include <memory>

#include <Statements/StatementHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{
namespace
{
class TopologyStatementHandlerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TopologyStatementHandlerTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(TopologyStatementHandlerTest, SetRecordingStorageUpdatesWorkerCatalog)
{
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(Host("localhost:8080"), {}, 8, {}, {}, 1024));

    TopologyStatementHandler handler(nullptr, workerCatalog);
    const auto result = handler(SetRecordingStorageStatement{.host = "localhost:8080", .recordingStorageBudget = 4096});

    ASSERT_TRUE(result.has_value()) << result.error().what();
    EXPECT_EQ(result->host, Host("localhost:8080"));
    EXPECT_EQ(result->recordingStorageBudget, 4096U);
    ASSERT_TRUE(workerCatalog->getWorker(Host("localhost:8080")).has_value());
    EXPECT_EQ(workerCatalog->getWorker(Host("localhost:8080"))->recordingStorageBudget, 4096U);
}

TEST_F(TopologyStatementHandlerTest, SetRecordingStorageRejectsUnknownWorker)
{
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    TopologyStatementHandler handler(nullptr, workerCatalog);

    const auto result = handler(SetRecordingStorageStatement{.host = "missing:8080", .recordingStorageBudget = 4096});

    ASSERT_FALSE(result.has_value());
}

TEST_F(TopologyStatementHandlerTest, ShowRecordingStorageListsAllWorkers)
{
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(Host("localhost:8081"), {}, 8, {}, {}, 2048));
    ASSERT_TRUE(workerCatalog->addWorker(Host("localhost:8080"), {}, 8, {}, {}, 1024));

    TopologyStatementHandler handler(nullptr, workerCatalog);
    const auto result = handler(ShowRecordingStorageStatement{.host = std::nullopt, .format = StatementOutputFormat::TEXT});

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result->workers.size(), 2U);
    EXPECT_EQ(result->workers[0].host, Host("localhost:8080"));
    EXPECT_EQ(result->workers[0].recordingStorageBudget, 1024U);
    EXPECT_EQ(result->workers[1].host, Host("localhost:8081"));
    EXPECT_EQ(result->workers[1].recordingStorageBudget, 2048U);
}

TEST_F(TopologyStatementHandlerTest, ShowRecordingStorageFiltersWorker)
{
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(Host("localhost:8080"), {}, 8, {}, {}, 1024));
    ASSERT_TRUE(workerCatalog->addWorker(Host("localhost:8081"), {}, 8, {}, {}, 2048));

    TopologyStatementHandler handler(nullptr, workerCatalog);
    const auto result = handler(ShowRecordingStorageStatement{.host = "localhost:8081", .format = std::nullopt});

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result->workers.size(), 1U);
    EXPECT_EQ(result->workers[0].host, Host("localhost:8081"));
    EXPECT_EQ(result->workers[0].recordingStorageBudget, 2048U);
}
}
}
