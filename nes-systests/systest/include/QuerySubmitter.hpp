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

#pragma once

#include <chrono>
#include <expected>
#include <optional>
#include <stop_token>
#include <unordered_set>
#include <vector>
#include <QueryManager/QueryManager.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

struct FinishedQueryPoll
{
    std::vector<DistributedQueryStatusSnapshot> completions;
    std::optional<Exception> error;
};

/// Interface for submitting queries to a NebulaStream Worker.
class QuerySubmitter
{
public:
    explicit QuerySubmitter(std::unique_ptr<QueryManager> queryManager);
    std::expected<DistributedQueryId, Exception> startQuery(
        const DistributedLogicalPlan& plan,
        std::chrono::steady_clock::time_point deadline,
        std::stop_token stopToken,
        std::chrono::milliseconds cancellationGracePeriod);
    void cleanupQuery(const DistributedQueryId& query, std::chrono::steady_clock::time_point deadline, std::stop_token stopToken);
    void releaseQuery(const DistributedQueryId& query);
    void cleanup(std::chrono::steady_clock::time_point deadline);
    void shutdown(std::chrono::steady_clock::time_point deadline);
    [[nodiscard]] std::optional<std::chrono::steady_clock::time_point> failedTeardownDeadline() const;
    FinishedQueryPoll pollFinishedQueriesRetained(
        std::chrono::steady_clock::time_point deadline, std::stop_token stopToken);

private:
    UniquePtr<QueryManager> queryManager;
    std::unordered_set<DistributedQueryId> ids;
    std::optional<std::chrono::steady_clock::time_point> failedCleanupDeadline;
};
}
