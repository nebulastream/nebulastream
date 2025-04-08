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

    [[nodiscard]] std::string toString() const;

    bool replaceOperator(const LogicalOperator& target, LogicalOperator replacement);

    template <class T>
    std::vector<T> getOperatorByType() const
    {
        std::vector<T> operators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            for (LogicalOperator op : BFSRange(rootOperator))
            {
                if (visitedOpIds.contains(op.getId()))
                {
                    continue;
                }
                visitedOpIds.insert(op.getId());
                if (op.tryGet<T>())
                {
                    operators.push_back(op.get<T>());
                }
            }
        }
        return operators;
    }

    template <typename... TraitTypes>
    std::vector<LogicalOperator> getOperatorsByTraits()
    {
        std::vector<LogicalOperator> matchingOperators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            for (auto op : BFSRange(rootOperator))
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

    /// Currently used to flip the plan (source -> sink) after parsing
    /// Once we have refactored the parser we can remove this function.
    std::unique_ptr<LogicalPlan> flip() const;

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
