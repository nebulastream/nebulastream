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
#include <map>
#include <set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <OperatorGraph.hpp>

namespace NES
{

/// Logical timestamp of a change log entry. Strictly increasing per change log.
using ChangeLogTimestamp = uint64_t;

/// A sub-plan of a query that has to be (re-)placed: all operators between the pinned upstream and the pinned downstream operators.
struct ChangeLogEntry
{
    /// Computes the partial order set (poset) of the sub-plan, i.e., all operators from the upstream to the downstream operators.
    static ChangeLogEntry
    create(std::set<OperatorId> upstreamOperators, std::set<OperatorId> downstreamOperators, const OperatorGraph& graph);

    std::set<OperatorId> upstreamOperators;
    std::set<OperatorId> downstreamOperators;
    /// All operators of the sub-plan including the upstream and downstream operators.
    std::set<OperatorId> poSetOfSubQueryPlan;

    bool operator==(const ChangeLogEntry&) const = default;
};

/// Records the sub-plans of a query that have to be (re-)placed. Before a placement amendment, overlapping entries are compacted into one
/// entry, so the amendment processes every operator only once.
///
/// Fixed issue: the original keys the entries by the wall-clock time in microseconds, so two entries recorded in the same microsecond
/// overwrite each other. This change log uses a logical timestamp instead.
class ChangeLog
{
public:
    /// Adds an entry and returns its timestamp.
    ChangeLogTimestamp addChangeLogEntry(ChangeLogEntry entry);

    /// Returns a timestamp that is larger than the timestamp of every entry added so far.
    [[nodiscard]] ChangeLogTimestamp getCurrentTimestamp() const { return nextTimestamp; }

    /// Compacts all entries before `timestamp` and returns the compacted entries.
    [[nodiscard]] std::vector<std::pair<ChangeLogTimestamp, ChangeLogEntry>>
    getCompactChangeLogEntriesBeforeTimestamp(ChangeLogTimestamp timestamp, const OperatorGraph& graph);

    /// Removes all entries before `timestamp`, as they have been processed.
    void updateProcessedChangeLogTimestamp(ChangeLogTimestamp timestamp);

    [[nodiscard]] std::vector<std::pair<ChangeLogTimestamp, ChangeLogEntry>>
    getChangeLogEntriesBeforeTimestamp(ChangeLogTimestamp timestamp) const;

    /// Merges overlapping entries into one entry, keeping the most upstream and most downstream operators.
    static ChangeLogEntry compactChangeLogEntries(const std::vector<ChangeLogEntry>& entries, const OperatorGraph& graph);

private:
    void performChangeLogCompactionTillTimestamp(ChangeLogTimestamp timestamp, const OperatorGraph& graph);
    void removeChangeLogsBefore(ChangeLogTimestamp timestamp);

    std::map<ChangeLogTimestamp, ChangeLogEntry> changeLogEntries;
    ChangeLogTimestamp nextTimestamp = 1;
    ChangeLogTimestamp lastProcessedChangeLogTimestamp = 0;
};

}
