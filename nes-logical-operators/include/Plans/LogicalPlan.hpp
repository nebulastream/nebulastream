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

/// The query plan encapsulates a set of logical operators and provides a set of utility functions.
class LogicalPlan
{
public:
    LogicalPlan() = default;
    explicit LogicalPlan(LogicalOperator rootOperator);
    explicit LogicalPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators);

    LogicalPlan(const LogicalPlan& other);
    LogicalPlan(LogicalPlan&& other);
    LogicalPlan& operator=(LogicalPlan&& other);

    /// Adds a new operator to the plan and promotes it as new root by reparenting existing root operators and replacing the current roots
    void promoteOperatorToRoot(LogicalOperator newRoot);

    /// Returns a string representation of the logical query plan
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// Replace `target` with `replacement`, keeping target's children
    bool replaceOperator(const LogicalOperator& target, LogicalOperator replacement);

    /// Replace `target` with `replacement`, keeping the children that are already inside `replacement`
    bool replaceOperatorExact(const LogicalOperator& target, LogicalOperator replacement);

    [[nodiscard]] std::vector<LogicalOperator> getParents(const LogicalOperator& target) const;

    template <class T>
    std::vector<T> getOperatorByType() const
    {
        std::vector<T> operators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            auto typedOps = BFSRange(rootOperator)
            | std::views::filter([&](const LogicalOperator& op)
            {
                if (visitedOpIds.contains(op.getId()))
                {
                    return false;
                }
                visitedOpIds.insert(op.getId());
                return op.tryGet<T>().has_value();
            })
            | std::views::transform([](const LogicalOperator& op)
            {
                return op.get<T>();
            });
            std::ranges::copy(typedOps, std::back_inserter(operators));
        }
        return operators;
    }

    template <typename... TraitTypes>
    std::vector<LogicalOperator> getOperatorsByTraits() const
    {
        std::vector<LogicalOperator> matchingOperators;
        std::set<OperatorId> visitedOpIds;

        for (auto const& rootOperator : rootOperators)
        {
            auto ops = BFSRange(rootOperator);

            auto filtered = ops
              | std::views::filter([&](LogicalOperator const& op) {
                  if (visitedOpIds.contains(op.getId()))
                      return false;
                  visitedOpIds.insert(op.getId());
                  return hasTraits<TraitTypes...>(op.getTraitSet());
                });

            std::ranges::copy(filtered, std::back_inserter(matchingOperators));
        }
        return matchingOperators;
    }

    /// Get all the leaf operators in the query plan (leaf operator is the one without any child)
    /// @note: in certain stages the source operators might not be Leaf operators
    std::vector<LogicalOperator> getLeafOperators() const;

    std::unordered_set<LogicalOperator> getAllOperators() const;

    [[nodiscard]] bool operator==(const LogicalPlan& otherPlan) const;
    friend std::ostream& operator<<(std::ostream& os, const LogicalPlan& plan);

    /// Holds the original SQL string
    std::string originalSql;

    std::vector<LogicalOperator> rootOperators{};
    QueryId queryId = INVALID_QUERY_ID;
};
}
FMT_OSTREAM(NES::LogicalPlan);
