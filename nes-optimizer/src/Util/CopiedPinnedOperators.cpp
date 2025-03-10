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

#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/CopiedPinnedOperators.hpp>
#include <algorithm>

namespace NES::Optimizer {

CopiedPinnedOperators
CopiedPinnedOperators::create(const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                              const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                              std::unordered_map<OperatorId, LogicalOperatorPtr>& operatorIdToOriginalOperatorMap) {

    std::set<LogicalOperatorPtr> copyOfPinnedUpStreamOperators;
    std::set<LogicalOperatorPtr> copyOfPinnedDownStreamOperators;

    // Temp container for iteration
    std::queue<LogicalOperatorPtr> operatorsToProcess;
    std::unordered_map<OperatorId, LogicalOperatorPtr> mapOfCopiedOperators;

    std::set<OperatorId> pinnedUpStreamOperatorId;
    for (auto pinnedUpStreamOperator : pinnedUpStreamOperators) {
        OperatorId operatorId = pinnedUpStreamOperator->getId();
        pinnedUpStreamOperatorId.emplace(operatorId);
        operatorsToProcess.emplace(pinnedUpStreamOperator);
        mapOfCopiedOperators[operatorId] = pinnedUpStreamOperator->copy()->as<LogicalOperator>();
    }

    std::set<OperatorId> pinnedDownStreamOperatorId;
    for (auto pinnedDownStreamOperator : pinnedDownStreamOperators) {
        pinnedDownStreamOperatorId.emplace(pinnedDownStreamOperator->getId());
    }

    while (!operatorsToProcess.empty()) {
        auto operatorToProcess = operatorsToProcess.front();
        operatorsToProcess.pop();
        // Skip if the topology node was visited previously
        OperatorId operatorId = operatorToProcess->getId();
        operatorIdToOriginalOperatorMap[operatorId] = operatorToProcess;
        LogicalOperatorPtr operatorCopy = mapOfCopiedOperators[operatorId];

        //If the operator is a pinned upstream operator then add to the set of pinned upstream copy
        if (pinnedUpStreamOperatorId.contains(operatorId)) {
            copyOfPinnedUpStreamOperators.emplace(operatorCopy);
        }

        //If the operator is a downstream operator then add to the set of pinned downstream copy
        if (pinnedDownStreamOperatorId.contains(operatorId)) {
            copyOfPinnedDownStreamOperators.emplace(operatorCopy);
        }

        const auto& downstreamOperators = operatorToProcess->getParents();
        std::for_each(downstreamOperators.begin(), downstreamOperators.end(), [&](const NodePtr& operatorNode) {
            // only process downstream operators that are either directly or indirectly connected to pinned downstream
            // operators
            bool connectedToPinnedDownstreamOperator =
                std::any_of(pinnedDownStreamOperators.begin(),
                            pinnedDownStreamOperators.end(),
                            [&](const auto& pinnedDownStreamOperator) {
                                OperatorId downStreamOperatorId = operatorNode->as<Operator>()->getId();
                                return pinnedDownStreamOperator->getId() == downStreamOperatorId
                                    || pinnedDownStreamOperator->getChildWithOperatorId(downStreamOperatorId);
                            });

            // Add the remaining operators if connected to pinned downstream operator
            if (connectedToPinnedDownstreamOperator) {

                auto parentOperator = operatorNode->as<LogicalOperator>();
                auto parentOperatorId = parentOperator->getId();
                LogicalOperatorPtr parentOperatorCopy;
                if (mapOfCopiedOperators.contains(parentOperatorId)) {
                    parentOperatorCopy = mapOfCopiedOperators[parentOperatorId];
                } else {
                    parentOperatorCopy = parentOperator->copy()->as<LogicalOperator>();
                    mapOfCopiedOperators[parentOperatorId] = parentOperatorCopy;
                }
                operatorCopy->addParent(parentOperatorCopy);
                operatorsToProcess.push(parentOperator);
            }
        });
    }
    return CopiedPinnedOperators(copyOfPinnedUpStreamOperators, copyOfPinnedDownStreamOperators);
}

CopiedPinnedOperators
CopiedPinnedOperators::createMinimalCopy(const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                              const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                              std::unordered_map<OperatorId, LogicalOperatorPtr>& operatorIdToOriginalOperatorMap)
{
    std::set<LogicalOperatorPtr> copyOfPinnedUpStreamOperators;
    std::set<LogicalOperatorPtr> copyOfPinnedDownStreamOperators;

    std::queue<LogicalOperatorPtr> operatorsToProcess;
    std::unordered_map<OperatorId, LogicalOperatorPtr> mapOfCopiedOperators;

    std::set<OperatorId> pinnedUpStreamOperatorIds;
    for (auto pinnedUpStreamOperator : pinnedUpStreamOperators) {
        pinnedUpStreamOperatorIds.emplace(pinnedUpStreamOperator->getId());
    }
    std::set<OperatorId> pinnedDownStreamOperatorIds;
    for (auto pinnedDownStreamOperator : pinnedDownStreamOperators) {
        pinnedDownStreamOperatorIds.emplace(pinnedDownStreamOperator->getId());
    }

    for (auto pinnedUpStreamOperator : pinnedUpStreamOperators) {
        auto opId = pinnedUpStreamOperator->getId();
        operatorsToProcess.emplace(pinnedUpStreamOperator);
        mapOfCopiedOperators[opId] = pinnedUpStreamOperator->copy()->as<LogicalOperator>();
    }

    while (!operatorsToProcess.empty()) {
        auto operatorToProcess = operatorsToProcess.front();
        operatorsToProcess.pop();

        OperatorId operatorId = operatorToProcess->getId();
        operatorIdToOriginalOperatorMap[operatorId] = operatorToProcess;

        LogicalOperatorPtr operatorCopy = mapOfCopiedOperators[operatorId];

        if (pinnedUpStreamOperatorIds.count(operatorId) > 0) {
            copyOfPinnedUpStreamOperators.emplace(operatorCopy);
        }
        if (pinnedDownStreamOperatorIds.count(operatorId) > 0) {
            copyOfPinnedDownStreamOperators.emplace(operatorCopy);
        }

        const auto& parents = operatorToProcess->getParents();
        for (auto& parentNode : parents) {
            auto parentOp = parentNode->as<LogicalOperator>();
            auto parentOpId = parentOp->getId();

            // 1) Check if connected to pinnedDownStreamOperator
            bool connectedToPinnedDownstreamOperator = std::any_of(
                pinnedDownStreamOperators.begin(),
                pinnedDownStreamOperators.end(),
                [&](const auto& pinnedDownStreamOperator) {
                    OperatorId pDownId = parentOpId;
                    return (pinnedDownStreamOperator->getId() == pDownId) ||
                           pinnedDownStreamOperator->getChildWithOperatorId(pDownId);
                });

            if (!connectedToPinnedDownstreamOperator) {
                continue;
            }

            // 2) Check if parent's state is pinned or not purely PLACED
            //    i.e. skip if (state == PLACED && !pinned).
            if (!shouldCopyParent(parentOp, pinnedUpStreamOperators, pinnedDownStreamOperators)) {
                continue;
            }

            // 3) If we get here, we DO want to copy/traverse the parent
            LogicalOperatorPtr parentOpCopy;
            if (mapOfCopiedOperators.contains(parentOpId)) {
                parentOpCopy = mapOfCopiedOperators[parentOpId];
            } else {
                parentOpCopy = parentOp->copy()->as<LogicalOperator>();
                mapOfCopiedOperators[parentOpId] = parentOpCopy;
            }

            operatorCopy->addParent(parentOpCopy);

            operatorsToProcess.push(parentOp);
        }
    }

    return CopiedPinnedOperators(copyOfPinnedUpStreamOperators, copyOfPinnedDownStreamOperators);
}

bool CopiedPinnedOperators::shouldCopyParent(const LogicalOperatorPtr& op,
                      const std::set<LogicalOperatorPtr>& pinnedUps,
                      const std::set<LogicalOperatorPtr>& pinnedDowns)
{
    if (!op) return false;

    if (pinnedUps.find(op) != pinnedUps.end())   return true;
    if (pinnedDowns.find(op) != pinnedDowns.end()) return true;

    auto state = op->getOperatorState();
    switch(state) {
        case OperatorState::TO_BE_PLACED:
        case OperatorState::TO_BE_REMOVED:
        case OperatorState::TO_BE_REPLACED:
            return true;
        default:
            // e.g. PLACED or REMOVED
                return false;
    }
}

CopiedPinnedOperators::CopiedPinnedOperators(const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                             const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators)
    : copiedPinnedUpStreamOperators(pinnedUpStreamOperators), copiedPinnedDownStreamOperators(pinnedDownStreamOperators) {}

}// namespace NES::Optimizer
