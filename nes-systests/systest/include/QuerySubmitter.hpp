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

#include <expected>
#include <unordered_set>
#include <vector>
#include <QueryManager/QueryManager.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

/// Interface for submitting queries to a NebulaStream Worker.
class QuerySubmitter
{
public:
    explicit QuerySubmitter(std::unique_ptr<QueryManager> queryManager);
    std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan);
    void startQuery(QueryId query);
    void stopQuery(QueryId query);
    void unregisterQuery(QueryId query);
    LocalQueryStatus waitForQueryTermination(QueryId query);

    /// Blocks until atleast one query has finished (or potentially failed)
    std::vector<LocalQueryStatus> finishedQueries();

private:
    UniquePtr<QueryManager> queryManager;
    std::unordered_set<QueryId> ids;
};
}
