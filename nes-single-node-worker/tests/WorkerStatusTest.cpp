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
#include <optional>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <WorkerStatus.hpp>

using namespace std::chrono_literals;

namespace NES
{
namespace
{
constexpr std::chrono::system_clock::time_point origin{1000s};

QueryId createQueryId(const int value)
{
    return QueryId::createLocal(LocalQueryId{fmt::format("00000000-0000-0000-0000-{:012}", value)});
}

LocalQueryStatusSnapshot
active(const QueryId& queryId, const QueryStatus state, const std::optional<std::chrono::system_clock::time_point> running)
{
    return {
        .queryId = queryId, .state = state, .metrics = {.start = origin, .running = running, .stop = std::nullopt, .error = std::nullopt}};
}

LocalQueryStatusSnapshot
terminated(const QueryId& queryId, const std::chrono::system_clock::time_point stop, const std::optional<Exception>& error)
{
    return {
        .queryId = queryId,
        .state = error ? QueryStatus::Failed : QueryStatus::Stopped,
        .metrics = {.start = origin, .running = origin, .stop = stop, .error = error}};
}
}

/// NOLINTBEGIN(readability-magic-numbers,bugprone-unchecked-optional-access)
TEST(WorkerStatusTest, ActiveAndTerminatedQueries)
{
    const auto status = buildWorkerStatus(
        {active(createQueryId(1), QueryStatus::Running, origin),
         active(createQueryId(2), QueryStatus::Started, std::nullopt),
         active(createQueryId(3), QueryStatus::Registered, std::nullopt),
         terminated(createQueryId(4), origin + 5s, std::nullopt),
         terminated(createQueryId(5), origin + 10s, std::nullopt),
         terminated(createQueryId(6), origin + 15s, QueryInvalid("crashed")),
         {.queryId = createQueryId(7),
          .state = QueryStatus::Failed,
          .metrics = {.start = origin, .running = std::nullopt, .stop = origin + 15s, .error = QueryInvalid("compile")}}},
        origin + 10s,
        origin + 20s);

    ASSERT_EQ(status.activeQueries.size(), 3);
    EXPECT_EQ(status.activeQueries[0].queryId, createQueryId(1));
    EXPECT_EQ(status.activeQueries[0].started, origin);
    EXPECT_EQ(status.activeQueries[1].queryId, createQueryId(2));
    EXPECT_FALSE(status.activeQueries[1].started.has_value());
    EXPECT_EQ(status.activeQueries[2].queryId, createQueryId(3));
    ASSERT_EQ(status.terminatedQueries.size(), 3);
    EXPECT_EQ(status.terminatedQueries[0].queryId, createQueryId(5));
    EXPECT_EQ(status.terminatedQueries[0].terminated, origin + 10s);
    EXPECT_FALSE(status.terminatedQueries[0].error.has_value());
    EXPECT_EQ(status.terminatedQueries[1].queryId, createQueryId(6));
    EXPECT_TRUE(status.terminatedQueries[1].error.has_value());
    EXPECT_EQ(status.terminatedQueries[2].queryId, createQueryId(7));
    EXPECT_FALSE(status.terminatedQueries[2].started.has_value());
    EXPECT_EQ(status.after, origin + 10s);
    EXPECT_EQ(status.until, origin + 20s);
}

/// NOLINTEND(readability-magic-numbers,bugprone-unchecked-optional-access)
}
