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
#include <Util/Pointers.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

/// Interface for submitting queries to a NebulaStream Worker.
class QuerySubmitter
{
public:
    explicit QuerySubmitter(std::unique_ptr<QueryManager> queryManager);
    std::expected<DistributedQueryId, Exception> registerQuery(const DistributedLogicalPlan& plan);
    void startQuery(const DistributedQueryId& query);
    void stopQuery(const DistributedQueryId& query);
    void unregisterQuery(const DistributedQueryId& query);
    DistributedQueryStatus waitForQueryTermination(const DistributedQueryId& query);

    /// Blocks until atleast one query has finished (or potentially failed)
    std::vector<DistributedQueryStatus> finishedQueries();

private:
    UniquePtr<QueryManager> queryManager;
    std::unordered_set<DistributedQueryId> ids;
};
}
