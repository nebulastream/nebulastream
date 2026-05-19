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
#include <unordered_map>
#include <vector>
#include <QueryManager/QueryManager.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

/// Interface for submitting queries to a NebulaStream Worker.
///
/// Each started query is subject to a wall-clock timeout. When the timeout elapses before the query reaches a terminal
/// state, the submitter best-effort stops the query and returns a synthetic Failed status snapshot carrying a
/// `TestException` describing the timeout, so the usual failure reporting path picks it up.
class QuerySubmitter
{
public:
    static constexpr std::chrono::milliseconds DEFAULT_QUERY_TIMEOUT = std::chrono::seconds{5};

    explicit QuerySubmitter(std::unique_ptr<QueryManager> queryManager, std::chrono::milliseconds queryTimeout = DEFAULT_QUERY_TIMEOUT);
    std::expected<DistributedQueryId, Exception> registerQuery(const DistributedLogicalPlan& plan);
    void startQuery(const DistributedQueryId& query);
    void stopQuery(const DistributedQueryId& query);
    DistributedQueryStatusSnapshot waitForQueryTermination(const DistributedQueryId& query);

    /// Blocks until atleast one query has finished (or potentially failed / timed out)
    std::vector<DistributedQueryStatusSnapshot> finishedQueries();

private:
    UniquePtr<QueryManager> queryManager;
    std::chrono::milliseconds queryTimeout;
    std::unordered_map<DistributedQueryId, std::chrono::steady_clock::time_point> startedAt;
};
}
