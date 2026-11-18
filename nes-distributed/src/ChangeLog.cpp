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

#include <ChangeLog.hpp>

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <ranges>
#include <set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>
#include <OperatorGraph.hpp>

namespace NES
{

ChangeLogEntry
ChangeLogEntry::create(std::set<OperatorId> upstreamOperators, std::set<OperatorId> downstreamOperators, const OperatorGraph& graph)
{
    ChangeLogEntry entry{
        .upstreamOperators = std::move(upstreamOperators),
        .downstreamOperators = std::move(downstreamOperators),
        .poSetOfSubQueryPlan = {}};
    if (entry.upstreamOperators.empty() or entry.downstreamOperators.empty())
    {
        return entry;
    }

    /// Visit all operators from the upstream operators towards the downstream operators.
    std::vector<OperatorId> operatorsToVisit(entry.upstreamOperators.begin(), entry.upstreamOperators.end());
    while (not operatorsToVisit.empty())
    {
        const auto visitingOperator = operatorsToVisit.back();
        operatorsToVisit.pop_back();
        if (not entry.poSetOfSubQueryPlan.insert(visitingOperator).second or entry.downstreamOperators.contains(visitingOperator))
        {
            continue;
        }
        const auto& downstreamOperatorsToVisit = graph.getDownstreamOperators(visitingOperator);
        if (downstreamOperatorsToVisit.size() == 1)
        {
            operatorsToVisit.emplace_back(downstreamOperatorsToVisit.front());
            continue;
        }
        /// With more than one downstream operator, only follow the paths that lead to one of the downstream operators of the entry.
        for (const auto& downstreamOperatorToVisit : downstreamOperatorsToVisit)
        {
            const bool visit
                = entry.downstreamOperators.contains(downstreamOperatorToVisit)
                or std::ranges::any_of(
                      entry.downstreamOperators,
                      [&](const auto& downstreamOperator) { return graph.isDownstreamOf(downstreamOperator, downstreamOperatorToVisit); });
            if (visit)
            {
                operatorsToVisit.emplace_back(downstreamOperatorToVisit);
            }
        }
    }
    return entry;
}

ChangeLogTimestamp ChangeLog::addChangeLogEntry(ChangeLogEntry entry)
{
    const auto timestamp = nextTimestamp++;
    changeLogEntries.emplace(timestamp, std::move(entry));
    return timestamp;
}

std::vector<std::pair<ChangeLogTimestamp, ChangeLogEntry>>
ChangeLog::getCompactChangeLogEntriesBeforeTimestamp(const ChangeLogTimestamp timestamp, const OperatorGraph& graph)
{
    performChangeLogCompactionTillTimestamp(timestamp, graph);
    return getChangeLogEntriesBeforeTimestamp(timestamp);
}

void ChangeLog::updateProcessedChangeLogTimestamp(const ChangeLogTimestamp timestamp)
{
    lastProcessedChangeLogTimestamp = timestamp;
    removeChangeLogsBefore(lastProcessedChangeLogTimestamp);
}

void ChangeLog::performChangeLogCompactionTillTimestamp(const ChangeLogTimestamp timestamp, const OperatorGraph& graph)
{
    auto entriesToCompact = getChangeLogEntriesBeforeTimestamp(timestamp);

    /// Repeat until all entries before the timestamp are non-overlapping.
    bool repeat = true;
    while (repeat)
    {
        repeat = false;
        for (size_t i = 0; i < entriesToCompact.size(); ++i)
        {
            const auto& candidate = entriesToCompact.at(i);
            std::vector<size_t> overlapping;
            for (size_t j = i + 1; j < entriesToCompact.size(); ++j)
            {
                std::vector<OperatorId> intersection;
                std::ranges::set_intersection(
                    candidate.second.poSetOfSubQueryPlan,
                    entriesToCompact.at(j).second.poSetOfSubQueryPlan,
                    std::back_inserter(intersection));
                if (not intersection.empty())
                {
                    overlapping.emplace_back(j);
                }
            }
            if (overlapping.empty())
            {
                continue;
            }

            std::vector<ChangeLogEntry> entriesToMerge{candidate.second};
            for (const auto index : overlapping)
            {
                entriesToMerge.emplace_back(entriesToCompact.at(index).second);
            }
            /// The compacted entry gets the timestamp of the candidate.
            auto compacted = std::pair{candidate.first, compactChangeLogEntries(entriesToMerge, graph)};
            for (auto index : overlapping | std::views::reverse)
            {
                entriesToCompact.erase(entriesToCompact.begin() + static_cast<std::ptrdiff_t>(index));
            }
            entriesToCompact.erase(entriesToCompact.begin() + static_cast<std::ptrdiff_t>(i));
            entriesToCompact.emplace_back(std::move(compacted));
            repeat = true;
            break;
        }
    }

    removeChangeLogsBefore(timestamp);
    for (auto& [entryTimestamp, entry] : entriesToCompact)
    {
        changeLogEntries.insert_or_assign(entryTimestamp, std::move(entry));
    }
}

std::vector<std::pair<ChangeLogTimestamp, ChangeLogEntry>>
ChangeLog::getChangeLogEntriesBeforeTimestamp(const ChangeLogTimestamp timestamp) const
{
    std::vector<std::pair<ChangeLogTimestamp, ChangeLogEntry>> entries;
    for (auto it = changeLogEntries.begin(); it != changeLogEntries.lower_bound(timestamp); ++it)
    {
        entries.emplace_back(it->first, it->second);
    }
    return entries;
}

void ChangeLog::removeChangeLogsBefore(const ChangeLogTimestamp timestamp)
{
    changeLogEntries.erase(changeLogEntries.begin(), changeLogEntries.lower_bound(timestamp));
}

namespace
{
/// Merges two sets of pinned operators. If an operator of one set is upstream (or downstream, for `keepUpstream == false`) of an
/// operator of the other set, only the more upstream (downstream) operator is kept.
///
/// Fixed issue: the original merges the sets by erasing from the sets while iterating them in the order of their pointer addresses, so
/// the result for chains of three operators depends on memory addresses. Here, an operator is dropped iff an operator of the other set
/// is more upstream (downstream), independent of any order.
std::set<OperatorId> mergePinnedOperators(
    const std::set<OperatorId>& first, const std::set<OperatorId>& next, const bool keepUpstream, const OperatorGraph& graph)
{
    /// Returns true if `op` has to be dropped in favour of an operator of `other`.
    auto isSuperseded = [&](const OperatorId op, const std::set<OperatorId>& other)
    {
        return std::ranges::any_of(
            other,
            [&](const OperatorId otherOp)
            { return otherOp != op and (keepUpstream ? graph.isDownstreamOf(op, otherOp) : graph.isDownstreamOf(otherOp, op)); });
    };
    std::set<OperatorId> merged;
    std::ranges::copy_if(first, std::inserter(merged, merged.end()), [&](const OperatorId op) { return not isSuperseded(op, next); });
    std::ranges::copy_if(next, std::inserter(merged, merged.end()), [&](const OperatorId op) { return not isSuperseded(op, first); });
    return merged;
}
}

ChangeLogEntry ChangeLog::compactChangeLogEntries(const std::vector<ChangeLogEntry>& entries, const OperatorGraph& graph)
{
    PRECONDITION(not entries.empty(), "Cannot compact an empty list of change log entries");
    auto upstreamOperators = entries.front().upstreamOperators;
    auto downstreamOperators = entries.front().downstreamOperators;
    for (const auto& entry : entries | std::views::drop(1))
    {
        upstreamOperators = mergePinnedOperators(upstreamOperators, entry.upstreamOperators, true, graph);
        downstreamOperators = mergePinnedOperators(downstreamOperators, entry.downstreamOperators, false, graph);
    }
    return ChangeLogEntry::create(std::move(upstreamOperators), std::move(downstreamOperators), graph);
}

}
