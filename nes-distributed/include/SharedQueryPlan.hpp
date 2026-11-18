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

#include <cstdint>
#include <optional>
#include <ostream>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ChangeLog.hpp>
#include <OperatorGraph.hpp>
#include <QueryId.hpp>

namespace NES
{

/// Placement state of a logical operator of a query (the original's `OperatorState`).
enum class OperatorState : uint8_t
{
    ToBePlaced,
    ToBeRemoved,
    ToBeReplaced,
    Placed,
    Removed,
};

/// Status of a shared query plan during a placement amendment (the original's `SharedQueryPlanStatus`).
enum class SharedQueryPlanStatus : uint8_t
{
    Created,
    Deployed,
    Updated,
    Stopped,
    Failed,
    /// Affected by a topology change and waiting for the placement amendment.
    Migrating,
    /// All change log entries were processed successfully.
    Processed,
    /// Some change log entries could not be processed, they were recorded again.
    PartiallyProcessed,
};

/// The global plan of a query together with the placement state of its operators and its change log.
/// The original merges queries with common sub-expressions into one shared query plan. The current code base has no query merging, so
/// every distributed query has exactly one shared query plan, and the shared query id is the distributed query id.
class SharedQueryPlan
{
public:
    /// Creates the shared query plan of a new query: all operators are to be placed, and the first change log entry covers the whole plan.
    SharedQueryPlan(DistributedQueryId id, LogicalPlan plan);

    /// Creates the shared query plan of a query that was placed by the initial (holistic) placement: all operators are placed on the
    /// given workers and the change log is empty.
    SharedQueryPlan(DistributedQueryId id, LogicalPlan plan, const std::unordered_map<OperatorId, Host>& placement);

    [[nodiscard]] const DistributedQueryId& getId() const { return id; }

    [[nodiscard]] const LogicalPlan& getPlan() const { return plan; }

    [[nodiscard]] const OperatorGraph& getGraph() const { return graph; }

    [[nodiscard]] OperatorState getOperatorState(OperatorId operatorId) const;
    void setOperatorState(OperatorId operatorId, OperatorState state);

    /// The worker an operator is placed on or pinned to (the original's `PINNED_WORKER_ID` property).
    [[nodiscard]] std::optional<Host> getPinnedWorker(OperatorId operatorId) const;
    void setPinnedWorker(OperatorId operatorId, Host worker);
    void removePinnedWorker(OperatorId operatorId);

    /// Marks all operators between the pinned upstream and downstream operators to be re-placed and records a change log entry.
    void performReOperatorPlacement(const std::set<OperatorId>& upstreamOperators, const std::set<OperatorId>& downstreamOperators);

    /// Returns a timestamp that is larger than the timestamps of all change log entries recorded so far.
    [[nodiscard]] ChangeLogTimestamp getCurrentChangeLogTimestamp() const { return changeLog.getCurrentTimestamp(); }

    /// Returns the compacted change log entries recorded before the timestamp.
    [[nodiscard]] std::vector<std::pair<ChangeLogTimestamp, ChangeLogEntry>> getChangeLogEntries(ChangeLogTimestamp timestamp);

    /// Removes all change log entries before the timestamp.
    void updateProcessedChangeLogTimestamp(ChangeLogTimestamp timestamp);

    /// Records the change log entries again whose processing failed. Each entry is widened to the most upstream and most downstream
    /// operators that are still placed, so the next amendment re-places the whole affected sub-plan.
    void recordFailedChangeLogEntries(const std::vector<ChangeLogEntry>& entries);

    [[nodiscard]] SharedQueryPlanStatus getStatus() const { return status; }

    void setStatus(SharedQueryPlanStatus newStatus) { status = newStatus; }

private:
    DistributedQueryId id;
    LogicalPlan plan;
    OperatorGraph graph;
    std::unordered_map<OperatorId, OperatorState> operatorStates;
    std::unordered_map<OperatorId, Host> pinnedWorkers;
    ChangeLog changeLog;
    SharedQueryPlanStatus status;
};

std::ostream& operator<<(std::ostream& os, OperatorState state);
std::ostream& operator<<(std::ostream& os, SharedQueryPlanStatus status);
}

FMT_OSTREAM(NES::OperatorState);
FMT_OSTREAM(NES::SharedQueryPlanStatus);
