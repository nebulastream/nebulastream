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

#include <SharedQueryPlan.hpp>

#include <algorithm>
#include <deque>
#include <optional>
#include <ostream>
#include <ranges>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ChangeLog.hpp>
#include <ErrorHandling.hpp>
#include <OperatorGraph.hpp>
#include <QueryId.hpp>

namespace NES
{

SharedQueryPlan::SharedQueryPlan(DistributedQueryId id, LogicalPlan plan)
    : id(std::move(id)), plan(std::move(plan)), graph(this->plan), status(SharedQueryPlanStatus::Created)
{
    for (const auto& operatorId : graph.getAllOperators())
    {
        operatorStates.emplace(operatorId, OperatorState::ToBePlaced);
    }
    auto leafs = graph.getLeafOperators();
    auto roots = graph.getRootOperators();
    changeLog.addChangeLogEntry(ChangeLogEntry::create({leafs.begin(), leafs.end()}, {roots.begin(), roots.end()}, graph));
}

SharedQueryPlan::SharedQueryPlan(DistributedQueryId id, LogicalPlan plan, const std::unordered_map<OperatorId, Host>& placement)
    : id(std::move(id)), plan(std::move(plan)), graph(this->plan), status(SharedQueryPlanStatus::Deployed)
{
    for (const auto& operatorId : graph.getAllOperators())
    {
        const auto it = placement.find(operatorId);
        PRECONDITION(it != placement.end(), "Operator {} of query {} has no placement", operatorId, this->id);
        operatorStates.emplace(operatorId, OperatorState::Placed);
        pinnedWorkers.emplace(operatorId, it->second);
    }
}

OperatorState SharedQueryPlan::getOperatorState(const OperatorId operatorId) const
{
    const auto it = operatorStates.find(operatorId);
    PRECONDITION(it != operatorStates.end(), "Operator {} is not part of query {}", operatorId, id);
    return it->second;
}

void SharedQueryPlan::setOperatorState(const OperatorId operatorId, const OperatorState state)
{
    PRECONDITION(operatorStates.contains(operatorId), "Operator {} is not part of query {}", operatorId, id);
    operatorStates[operatorId] = state;
}

std::optional<Host> SharedQueryPlan::getPinnedWorker(const OperatorId operatorId) const
{
    if (const auto it = pinnedWorkers.find(operatorId); it != pinnedWorkers.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void SharedQueryPlan::setPinnedWorker(const OperatorId operatorId, Host worker)
{
    PRECONDITION(operatorStates.contains(operatorId), "Operator {} is not part of query {}", operatorId, id);
    pinnedWorkers.insert_or_assign(operatorId, std::move(worker));
}

void SharedQueryPlan::removePinnedWorker(const OperatorId operatorId)
{
    pinnedWorkers.erase(operatorId);
}

void SharedQueryPlan::performReOperatorPlacement(
    const std::set<OperatorId>& upstreamOperators, const std::set<OperatorId>& downstreamOperators)
{
    for (const auto& operatorToRePlace : graph.findAllOperatorsBetween(downstreamOperators, upstreamOperators))
    {
        setOperatorState(operatorToRePlace, OperatorState::ToBeReplaced);
    }
    changeLog.addChangeLogEntry(ChangeLogEntry::create(upstreamOperators, downstreamOperators, graph));
}

std::vector<std::pair<ChangeLogTimestamp, ChangeLogEntry>> SharedQueryPlan::getChangeLogEntries(const ChangeLogTimestamp timestamp)
{
    return changeLog.getCompactChangeLogEntriesBeforeTimestamp(timestamp, graph);
}

void SharedQueryPlan::updateProcessedChangeLogTimestamp(const ChangeLogTimestamp timestamp)
{
    changeLog.updateProcessedChangeLogTimestamp(timestamp);
}

void SharedQueryPlan::recordFailedChangeLogEntries(const std::vector<ChangeLogEntry>& entries)
{
    for (const auto& entry : entries)
    {
        /// Find the most downstream operators: walk upstream from the downstream operators until an operator has an upstream operator
        /// that is not placed.
        std::set<OperatorId> newDownstreamOperators;
        std::deque<OperatorId> downstreamOperatorsToCheck(entry.downstreamOperators.begin(), entry.downstreamOperators.end());
        while (not downstreamOperatorsToCheck.empty())
        {
            const auto downstreamOperator = downstreamOperatorsToCheck.front();
            downstreamOperatorsToCheck.pop_front();
            const auto& upstreamOperators = graph.getUpstreamOperators(downstreamOperator);
            if (std::ranges::any_of(upstreamOperators, [this](auto op) { return getOperatorState(op) != OperatorState::Placed; }))
            {
                newDownstreamOperators.insert(downstreamOperator);
                continue;
            }
            downstreamOperatorsToCheck.insert(downstreamOperatorsToCheck.end(), upstreamOperators.begin(), upstreamOperators.end());
        }
        if (newDownstreamOperators.empty())
        {
            NES_ERROR("Unable to find the most downstream operator of a failed change log entry of query {}. The entry is dropped.", id);
            continue;
        }

        /// Find the most upstream operators: walk downstream from the upstream operators until an operator has a downstream operator
        /// that is not placed.
        std::set<OperatorId> newUpstreamOperators;
        std::deque<OperatorId> upstreamOperatorsToCheck(entry.upstreamOperators.begin(), entry.upstreamOperators.end());
        while (not upstreamOperatorsToCheck.empty())
        {
            const auto upstreamOperator = upstreamOperatorsToCheck.front();
            upstreamOperatorsToCheck.pop_front();
            const auto& downstreamOperators = graph.getDownstreamOperators(upstreamOperator);
            if (std::ranges::any_of(downstreamOperators, [this](auto op) { return getOperatorState(op) != OperatorState::Placed; }))
            {
                newUpstreamOperators.insert(upstreamOperator);
                continue;
            }
            upstreamOperatorsToCheck.insert(upstreamOperatorsToCheck.end(), downstreamOperators.begin(), downstreamOperators.end());
        }
        if (newUpstreamOperators.empty())
        {
            NES_ERROR("Unable to find the most upstream operator of a failed change log entry of query {}. The entry is dropped.", id);
            continue;
        }

        changeLog.addChangeLogEntry(ChangeLogEntry::create(std::move(newUpstreamOperators), std::move(newDownstreamOperators), graph));
    }
}

std::ostream& operator<<(std::ostream& os, const OperatorState state)
{
    return os << magic_enum::enum_name(state);
}

std::ostream& operator<<(std::ostream& os, const SharedQueryPlanStatus status)
{
    return os << magic_enum::enum_name(status);
}

}
