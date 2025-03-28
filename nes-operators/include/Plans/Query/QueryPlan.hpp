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
#include <Plans/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{


/// The query plan encapsulates a set of operators and provides a set of utility functions.
class QueryPlan
{
public:
    QueryPlan() = default;
    explicit QueryPlan(LogicalOperator rootOperator);
    explicit QueryPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators);
    static std::unique_ptr<QueryPlan> create(LogicalOperator rootOperator);
    static std::unique_ptr<QueryPlan> create(QueryId queryId, std::vector<LogicalOperator> rootOperators);

    QueryPlan(const QueryPlan& other);
    QueryPlan(QueryPlan&& other);
    QueryPlan& operator=(QueryPlan&& other);

    /// Operator is being promoted as the new root by reparenting existing root operators and replacing the current roots
    void promoteOperatorToRoot(LogicalOperator newRoot);
    /// Adds operator to roots vector
    void addRootOperator(LogicalOperator newRootOperator);

    [[nodiscard]] std::string toString() const;

    std::vector<LogicalOperator> getRootOperators() const;

    template <typename... TraitTypes>
    std::vector<LogicalOperator> getOperatorsWithTraits()
    {
        std::vector<LogicalOperator> matchingOperators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            for (auto op : BFSRange<LogicalOperator>(rootOperator))
            {
                if (!visitedOpIds.contains(op.getId()))
                {
                    visitedOpIds.insert(op.getId());
                    if (hasTraits<TraitTypes...>(op.getTraitSet()))
                    {
                        matchingOperators.emplace_back(op);
                    }
                }
            }
        }
        return matchingOperators;
    }

    bool replaceOperator(const LogicalOperator& target, const LogicalOperator& replacement)
    {
        bool replaced = false;

        for (auto& root : rootOperators)
        {
            if (root.getId() == target.getId())
            {
                root = replacement;
                replaced = true;
            }
            else if (replaceOperatorRecursively(root, target, replacement))
            {
                replaced = true;
            }
        }

        return replaced;
    }

    bool replaceOperatorRecursively(LogicalOperator& current, const LogicalOperator& target, const LogicalOperator& replacement)
    {
        bool replaced = false;
        auto children = current.getChildren();

        for (size_t i = 0; i < children.size(); ++i)
        {
            if (children[i].getId() == target.getId())
            {
                children[i] = replacement;
                replaced = true;
            }
            else if (replaceOperatorRecursively(children[i], target, replacement))
            {
                replaced = true;
            }
        }

        if (replaced)
        {
            current.setChildren(children);
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

    std::unique_ptr<QueryPlan> flip() const
    {
        std::unordered_map<uint64_t, LogicalOperator> flippedOperators;
        std::unordered_map<uint64_t, std::vector<uint64_t>> parentMapping;

        /// Collect all operators without children
        for (const auto& root : rootOperators)
        {
            for (const auto& op : BFSRange<LogicalOperator>(root))
            {
                auto opIdRaw = op.getId().getRawValue();
                if (!flippedOperators.contains(opIdRaw))
                {
                    LogicalOperator opCopy = op;
                    opCopy.setChildren({});
                    flippedOperators.emplace(opIdRaw, std::move(opCopy));
                }

                for (const auto& child : op.getChildren())
                {
                    auto childIdRaw = child.getId().getRawValue();
                    parentMapping[childIdRaw].push_back(opIdRaw);
                }
            }
        }

        /// Build the flipped plan
        for (const auto& [childId, parentIds] : parentMapping)
        {
            std::vector<LogicalOperator> parents;
            for (auto pid : parentIds)
            {
                parents.push_back(flippedOperators[pid]);
            }
            flippedOperators[childId].setChildren(parents);
        }

        /// New root nodes
        std::unordered_set<uint64_t> nonRoots;
        for (const auto& [_, parents] : parentMapping)
        {
            for (auto parentId : parents)
            {
                nonRoots.insert(parentId);
            }
        }

        std::vector<LogicalOperator> newRoots;
        for (const auto& [id, op] : flippedOperators)
        {
            if (!nonRoots.contains(id))
            {
                newRoots.push_back(op);
            }
        }

        return std::make_unique<QueryPlan>(getQueryId(), newRoots);
    }


    /// Get all the leaf operators in the query plan (leaf operator is the one without any child)
    /// @note: in certain stages the source operators might not be Leaf operators
    std::vector<LogicalOperator> getLeafOperators() const;

    std::unordered_set<LogicalOperator> getAllOperators() const;

    void setQueryId(QueryId queryId);
    [[nodiscard]] QueryId getQueryId() const;

    [[nodiscard]] bool operator==(const QueryPlan& otherPlan) const;

private:
    /// Owning pointers to the operators
    std::vector<LogicalOperator> rootOperators{};
    QueryId queryId = INVALID_QUERY_ID;
};
}
