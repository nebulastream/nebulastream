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

#include <memory>
#include <set>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{


/// The query plan encapsulates a set of operators and provides a set of utility functions.
class LogicalPlan
{
public:
    LogicalPlan() = default;
    explicit LogicalPlan(LogicalOperator rootOperator);
    explicit LogicalPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators);
    static std::unique_ptr<LogicalPlan> create(LogicalOperator rootOperator);
    static std::unique_ptr<LogicalPlan> create(QueryId queryId, std::vector<LogicalOperator> rootOperators);

    LogicalPlan(const LogicalPlan& other);
    LogicalPlan(LogicalPlan&& other);
    LogicalPlan& operator=(LogicalPlan&& other);

    /// Operator is being promoted as the new root by reparenting existing root operators and replacing the current roots
    void promoteOperatorToRoot(LogicalOperator newRoot);
    /// Adds operator to roots vector
    void addRootOperator(LogicalOperator newRootOperator);

    [[nodiscard]] std::string toString() const;

    std::vector<LogicalOperator> getRootOperators() const;

    template <typename... TraitTypes>
    std::vector<LogicalOperator> getOperatorsWithTraits() {
        std::vector<LogicalOperator> matchingOperators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators) {
            for (auto op : BFSRange<LogicalOperator>(rootOperator)) {
                if (!visitedOpIds.contains(op.getId())) {
                    visitedOpIds.insert(op.getId());
                    if (hasTraits<TraitTypes...>(op.getTraitSet())) {
                        matchingOperators.emplace_back(op);
                    }
                }
            }
        }
        return matchingOperators;
    }

    bool replaceOperator(LogicalOperator& current,
                         const LogicalOperator& target,
                         LogicalOperator replacement) {
        if (current.getId() == target.getId()) {
            replacement.setChildren(current.getChildren());
            current = replacement;
            return true;
        }
        bool replaced = false;
        auto children = current.getChildren();
        for (size_t i = 0; i < children.size(); ++i) {
            if (replaceOperator(children[i], target, replacement)) {
                replaced = true;
            }
        }
        if (replaced) {
            current.setChildren(children);
        }
        return replaced;
    }

    bool replaceOperator(const LogicalOperator& target, LogicalOperator replacement) {
        bool replaced = false;
        for (auto& root : rootOperators) {
            if (root.getId() == target.getId()) {
                replacement.setChildren(root.getChildren());
                root = replacement;

                replaced = true;
            } else if (replaceOperator(root, target, replacement)) {
                replaced = true;
            }
        }
        return replaced;
    }

    template <class T>
    std::vector<T> getOperatorByType() const
    {
        std::vector<T> operators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            for (LogicalOperator op : BFSRange<LogicalOperator>(rootOperator))
            {
                if (visitedOpIds.contains(op.getId()))
                {
                    continue;
                }
                visitedOpIds.insert(op.getId());
                if (op.tryGet<T>())
                {
                    operators.push_back(*op.get<T>());
                }
            }
        }
        return operators;
    }

    std::unique_ptr<LogicalPlan> flip() const
    {
        std::unordered_map<OperatorId, std::vector<LogicalOperator>> reversedEdges;
        std::unordered_map<OperatorId, LogicalOperator> idToOperator;
        std::set<OperatorId> allOperators;

        for (const auto& root : rootOperators) {
            for (auto op : BFSRange<LogicalOperator>(root)) {
                idToOperator[op.getId()] = op;
                allOperators.insert(op.getId());
                for (const auto& child : op.getChildren()) {
                    reversedEdges[child.getId()].push_back(op);
                }
            }
        }

        std::vector<LogicalOperator> newRoots;
        for (const auto& id : allOperators) {
            if (idToOperator[id].getChildren().empty()) {
                newRoots.push_back(idToOperator[id]);
            }
        }

        std::unordered_map<OperatorId, LogicalOperator> flippedOperators;
        std::unordered_set<OperatorId> visited;

        std::function<LogicalOperator(const LogicalOperator&)> flipOperator;
        flipOperator = [&](const LogicalOperator& op) -> LogicalOperator {
            auto opId = op.getId();
            if (visited.contains(opId)) {
                return flippedOperators[opId];
            }
            visited.insert(opId);

            LogicalOperator flippedOp = op;
            flippedOp.setChildren({});
            flippedOperators[opId] = flippedOp;

            std::vector<LogicalOperator> newChildren;
            for (const auto& parent : reversedEdges[opId]) {
                newChildren.push_back(flipOperator(parent));
            }
            flippedOperators[opId].setChildren(newChildren);

            return flippedOperators[opId];
        };

        std::vector<LogicalOperator> flippedRoots;
        std::unordered_set<OperatorId> rootIds;
        for (const auto& root : newRoots) {
            if (!rootIds.contains(root.getId())) {
                flippedRoots.push_back(flipOperator(root));
                rootIds.insert(root.getId());
            }
        }

        return std::make_unique<LogicalPlan>(getQueryId(), flippedRoots);
    }


    /// Get all the leaf operators in the query plan (leaf operator is the one without any child)
    /// @note: in certain stages the source operators might not be Leaf operators
    std::vector<LogicalOperator> getLeafOperators() const;

    std::unordered_set<LogicalOperator> getAllOperators() const;

    void setQueryId(QueryId queryId);
    [[nodiscard]] QueryId getQueryId() const;

    [[nodiscard]] bool operator==(const LogicalPlan& otherPlan) const;

    std::vector<LogicalOperator> rootOperators{};
private:
    QueryId queryId = INVALID_QUERY_ID;
};
}
