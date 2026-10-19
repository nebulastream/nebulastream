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
#include <memory>
#include <unordered_map>
#include <vector>
#include <QueryManager/QueryManager.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// A query that is no longer running: it stopped, failed, or ran past the timeout and the submitter stopped it.
struct FinishedQuery
{
    DistributedQueryId id;
    std::expected<DistributedQueryStatusSnapshot, Exception> outcome;
};

/// Interface for submitting queries to a NebulaStream Worker.
class QuerySubmitter
{
public:
    /// The timeout bounds how long a submitted query may take to reach a terminal state.
    /// Zero disables the timeout.
    QuerySubmitter(std::unique_ptr<QueryManager> queryManager, std::chrono::milliseconds timeout);
    std::expected<DistributedQueryId, Exception> startQuery(const DistributedLogicalPlan& plan);
    void stopQuery(const DistributedQueryId& query);
    DistributedQueryStatusSnapshot waitForQueryTermination(const DistributedQueryId& query);

    /// Blocks until at least one query is finished, and returns every finished query.
    /// The submitter stops a query past the timeout and reports it as a timeout error.
    /// A query that never ends then fails its test instead of blocking the whole run.
    std::vector<FinishedQuery> finishedQueries();

private:
    UniquePtr<QueryManager> queryManager;
    std::chrono::milliseconds timeout;
    /// The running queries and when each was submitted, which the timeout counts from.
    std::unordered_map<DistributedQueryId, std::chrono::steady_clock::time_point> running;
};
}
