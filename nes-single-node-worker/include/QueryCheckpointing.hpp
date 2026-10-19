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

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <CompiledQueryPlan.hpp>
#include <QueryId.hpp>

namespace NES
{

/// Keeps the serialized plan of every checkpointed query in the query's checkpoint directory, so that recovery can recompile it, and
/// writes the state of the query's checkpointable operator handlers in every checkpoint round. The directory of a query is removed once
/// the query stopped.
class QueryCheckpointing
{
public:
    QueryCheckpointing(std::shared_ptr<CheckpointManager> checkpointManager, std::shared_ptr<const QueryLog> queryLog);
    ~QueryCheckpointing();

    QueryCheckpointing(const QueryCheckpointing&) = delete;
    QueryCheckpointing& operator=(const QueryCheckpointing&) = delete;
    QueryCheckpointing(QueryCheckpointing&&) = delete;
    QueryCheckpointing& operator=(QueryCheckpointing&&) = delete;

    struct RecoverableQuery
    {
        LogicalPlan plan;
        std::filesystem::path queryDirectory;
    };

    /// Skips queries whose plan cannot be read.
    [[nodiscard]] std::vector<RecoverableQuery> findRecoverableQueries() const;

    /// Must be called before the query starts. The handlers of a recovered query restore the state of the recovered checkpoint round.
    void registerQuery(const LogicalPlan& plan, const CompiledQueryPlan& compiledQueryPlan);
    /// For a registered query that could not be started. Removes its checkpoint.
    void unregisterQuery(const QueryId& queryId);

private:
    void removeTerminatedQueries();
    void removeQuery(const QueryId& queryId, bool removeCheckpoint);

    std::shared_ptr<CheckpointManager> checkpointManager;
    std::shared_ptr<const QueryLog> queryLog;
    std::mutex mutex;
    std::vector<QueryId> checkpointedQueries;
};

}
