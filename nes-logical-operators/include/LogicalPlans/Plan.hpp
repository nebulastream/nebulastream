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
#include <LogicalOperators/Sinks/SinkOperator.hpp>
#include <Util/Common.hpp>

namespace NES::Logical
{

/// The query plan encapsulates a set of logical operators and provides a set of utility functions.
class Plan
{
public:
    Plan() = default;
    explicit Plan(Operator rootOperator);
    explicit Plan(QueryId queryId, std::vector<Operator> rootOperators);

    Plan(const Plan& other);
    Plan(Plan&& other);
    Plan& operator=(Plan&& other);

    /// Adds a new operator to the plan and promotes it as new root by reparenting existing root operators and replacing the current roots
    void promoteOperatorToRoot(Operator newRoot);

    /// Returns a string representation of the logical query plan
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// Replace `target` with `replacement`, keeping target's children
    bool replaceOperator(const Operator& target, Operator replacement);

    /// Replace `target` with `replacement`, keeping the children that are already inside `replacement`
    bool replaceOperatorExact(const Operator& target, Operator replacement);

    [[nodiscard]] std::vector<Operator> getParents(const Operator& target) const;

    template <class T>
    std::vector<T> getOperatorByType() const
    {
        std::vector<T> operators;
        std::set<OperatorId> visitedOpIds;
        for (const auto& rootOperator : rootOperators)
        {
            auto typedOps = BFSRange(rootOperator)
            | std::views::filter([&](const Operator& op)
            {
                if (visitedOpIds.contains(op.getId()))
                {
                    return false;
                }
                visitedOpIds.insert(op.getId());
                return op.tryGet<T>().has_value();
            })
            | std::views::transform([](const Operator& op)
            {
                return op.get<T>();
            });
            std::ranges::copy(typedOps, std::back_inserter(operators));
        }
        return operators;
    }

    template <typename... TraitTypes>
    std::vector<Operator> getOperatorsByTraits() const
    {
        std::vector<Operator> matchingOperators;
        std::set<OperatorId> visitedOpIds;

        for (auto const& rootOperator : rootOperators)
        {
            auto ops = BFSRange(rootOperator);

            auto filtered = ops
              | std::views::filter([&](Operator const& op) {
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
    std::vector<Operator> getLeafOperators() const;

    std::unordered_set<Operator> getAllOperators() const;

    [[nodiscard]] bool operator==(const Plan& otherPlan) const;
    friend std::ostream& operator<<(std::ostream& os, const Plan& plan);

    /// Holds the original SQL string
    std::string originalSql;

    std::vector<Operator> rootOperators{};
    QueryId queryId = INVALID_QUERY_ID;
};
}
FMT_OSTREAM(NES::Logical::Plan);
