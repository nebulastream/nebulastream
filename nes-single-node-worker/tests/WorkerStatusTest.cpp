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

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>
#include <QueryStatus.hpp>
#include <WorkerStatus.hpp>

using namespace std::chrono_literals;

namespace NES
{
namespace
{
const std::chrono::system_clock::time_point origin{1000s};

LocalQueryStatusSnapshot
active(const QueryId queryId, const QueryStatus state, const std::optional<std::chrono::system_clock::time_point> running)
{
    return {
        .queryId = queryId, .state = state, .metrics = {.start = origin, .running = running, .stop = std::nullopt, .error = std::nullopt}};
}

LocalQueryStatusSnapshot
terminated(const QueryId queryId, const std::chrono::system_clock::time_point stop, const std::optional<Exception>& error)
{
    return {
        .queryId = queryId,
        .state = error ? QueryStatus::Failed : QueryStatus::Stopped,
        .metrics = {.start = origin, .running = origin, .stop = stop, .error = error}};
}
}

/// NOLINTBEGIN(readability-magic-numbers,bugprone-unchecked-optional-access)
TEST(WorkerStatusTest, QueryThatStartedBeforeAfterIsStillActive)
{
    const auto status = buildWorkerStatus(
        {active(QueryId{1}, QueryStatus::Running, origin),
         active(QueryId{2}, QueryStatus::Started, std::nullopt),
         active(QueryId{3}, QueryStatus::Registered, std::nullopt)},
        origin + 10s,
        origin + 20s);

    ASSERT_EQ(status.activeQueries.size(), 3);
    EXPECT_EQ(status.activeQueries[0].queryId, QueryId{1});
    EXPECT_EQ(status.activeQueries[0].started, origin);
    EXPECT_EQ(status.activeQueries[1].queryId, QueryId{2});
    EXPECT_FALSE(status.activeQueries[1].started.has_value());
    EXPECT_EQ(status.activeQueries[2].queryId, QueryId{3});
    EXPECT_TRUE(status.terminatedQueries.empty());
    EXPECT_EQ(status.after, origin + 10s);
    EXPECT_EQ(status.until, origin + 20s);
}

TEST(WorkerStatusTest, OnlyQueriesThatTerminatedAtOrAfterAfterAreListed)
{
    const auto status = buildWorkerStatus(
        {terminated(QueryId{1}, origin + 5s, std::nullopt),
         terminated(QueryId{2}, origin + 10s, std::nullopt),
         terminated(QueryId{3}, origin + 15s, QueryInvalid("crashed"))},
        origin + 10s,
        origin + 20s);

    EXPECT_TRUE(status.activeQueries.empty());
    ASSERT_EQ(status.terminatedQueries.size(), 2);
    EXPECT_EQ(status.terminatedQueries[0].queryId, QueryId{2});
    EXPECT_EQ(status.terminatedQueries[0].terminated, origin + 10s);
    EXPECT_FALSE(status.terminatedQueries[0].error.has_value());
    EXPECT_EQ(status.terminatedQueries[1].queryId, QueryId{3});
    EXPECT_TRUE(status.terminatedQueries[1].error.has_value());
}

/// NOLINTEND(readability-magic-numbers,bugprone-unchecked-optional-access)
}
