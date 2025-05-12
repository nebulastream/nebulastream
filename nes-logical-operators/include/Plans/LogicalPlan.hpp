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

#include <algorithm>
#include <iterator>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES
{

/// The logical plan encapsulates a set of logical operators that belong to exactly one query.
class LogicalPlan
{
public:
    LogicalPlan() = default;
    explicit LogicalPlan(LogicalOperator rootOperator);
    explicit LogicalPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators);
    explicit LogicalPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators, std::string originalSql);

    LogicalPlan(const LogicalPlan& other);
    LogicalPlan& operator=(const LogicalPlan& other);
    LogicalPlan(LogicalPlan&& other) noexcept;
    LogicalPlan& operator=(LogicalPlan&& other) noexcept;

    [[nodiscard]] bool operator==(const LogicalPlan& otherPlan) const;
    friend std::ostream& operator<<(std::ostream& os, const LogicalPlan& plan);

    [[nodiscard]] QueryId getQueryId() const;
    [[nodiscard]] std::string getOriginalSql() const;
    [[nodiscard]] std::vector<LogicalOperator> getRootOperators() const;

    void setRootOperators(const std::vector<LogicalOperator>& operators);
    void setOriginalSql(const std::string& sql);
    void setQueryId(QueryId id);
private:
    QueryId queryId = INVALID_QUERY_ID;
    std::vector<LogicalOperator> rootOperators;
    std::string originalSql; /// Holds the original SQL string
};

/// Get all parent operators of the target operator
[[nodiscard]] std::vector<LogicalOperator> getParents(const LogicalPlan& plan, const LogicalOperator& target);

/// Replace `target` with `replacement`, keeping target's children
[[nodiscard]] std::optional<LogicalPlan>
replaceOperator(const LogicalPlan& plan, OperatorId target, LogicalOperator replacement);

/// Replace `target` with `replacement`, keeping the children that are already inside `replacement`
[[nodiscard]] std::optional<LogicalPlan>
replaceSubtree(const LogicalPlan& plan, OperatorId target, const LogicalOperator& replacement);

/// Adds a new operator to the plan and promotes it as new root by reparenting existing root operators and replacing the current roots
[[nodiscard]] LogicalPlan promoteOperatorToRoot(const LogicalPlan& plan, const LogicalOperator& newRoot);

template <class T>
[[nodiscard]] std::vector<T> getOperatorByType(const LogicalPlan& plan)
{
    std::vector<T> operators;
    std::ranges::for_each(
        plan.getRootOperators(),
        [&operators](const auto& rootOperator)
        {
            auto typedOps = BFSRange(rootOperator)
                | std::views::filter([&](const LogicalOperator& op) { return op.tryGet<T>().has_value(); })
                | std::views::transform([](const LogicalOperator& op) { return op.get<T>(); });
            std::ranges::copy(typedOps, std::back_inserter(operators));
        });
    return operators;
}

[[nodiscard]] std::optional<LogicalOperator> getOperatorById(const LogicalPlan& plan, OperatorId operatorId);

template <typename... TraitTypes>
[[nodiscard]] std::vector<LogicalOperator> getOperatorsByTraits(const LogicalPlan& plan)
{
    std::vector<LogicalOperator> matchingOperators;
    std::ranges::for_each(
        plan.getRootOperators(),
        [&matchingOperators](const auto& rootOperator)
        {
            auto ops = BFSRange(rootOperator);
            auto filtered = ops | std::views::filter([&](const LogicalOperator& op) { return hasTraits<TraitTypes...>(op.getTraitSet()); });

            std::ranges::copy(filtered, std::back_inserter(matchingOperators));
        });
    return matchingOperators;
}

/// Returns a string representation of the logical query plan
[[nodiscard]] std::string explain(const LogicalPlan& plan, ExplainVerbosity verbosity);

/// Get all the leaf operators in the query plan (leaf operator is the one without any child)
/// @note: in certain stages the source operators might not be Leaf operators
[[nodiscard]] std::vector<LogicalOperator> getLeafOperators(const LogicalPlan& plan);

/// Returns a set of all operators
[[nodiscard]] std::unordered_set<LogicalOperator> flatten(const LogicalPlan& plan);

}
FMT_OSTREAM(NES::LogicalPlan);
