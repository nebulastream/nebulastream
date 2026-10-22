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

#include <Rules/Static/OriginSplitInsertionRule.hpp>

#include <cstddef>
#include <set>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginSplitLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{
/// How many consumers read each operator, counted before any split is inserted. Operators compare by identity, so two
/// branches that happen to hold equal operators are counted apart.
std::unordered_map<LogicalOperator, size_t> countConsumers(const LogicalPlan& plan)
{
    std::unordered_map<LogicalOperator, size_t> consumers;
    for (const auto& op : planOperators(plan))
    {
        for (const auto& child : op.getChildren())
        {
            ++consumers[child];
        }
    }
    return consumers;
}

/// Heads every branch of this operator that reads a fan-out point with a split of its own.
LogicalOperator headBranchesWithSplits(
    const LogicalOperator& op, std::vector<LogicalOperator> children, const std::unordered_map<LogicalOperator, size_t>& consumers)
{
    /// A leaf has nothing below it that could have changed, and operators that take no children reject being rebuilt
    /// with them at all, so it is handed back untouched.
    if (children.empty())
    {
        return op;
    }

    /// A split already heads its branch, so what it reads is the shared operator itself and must not be wrapped again.
    /// The check belongs on the consumer, not on the child: after a run, the shared operator is read by one split per
    /// branch and so still has several consumers, and checking the child would wrap it again on every further run.
    if (op.tryGetAs<OriginSplitLogicalOperator>().has_value())
    {
        return op.withChildrenUnsafe(std::move(children));
    }

    /// The rebuilt children arrive in the order of the original ones, so the consumer count of the original child
    /// decides whether the branch leading to it needs an identity of its own.
    const auto originalChildren = op.getChildren();
    INVARIANT(
        originalChildren.size() == children.size(),
        "An operator keeps its number of children while the plan is rebuilt, but got {} and {}",
        originalChildren.size(),
        children.size());

    for (size_t index = 0; index < children.size(); ++index)
    {
        const auto consumerCount = consumers.find(originalChildren.at(index));
        if (consumerCount == consumers.end() or consumerCount->second < 2)
        {
            continue;
        }
        children[index] = OriginSplitLogicalOperator::create(children.at(index));
    }
    /// A split forwards the records of its child unchanged, so no schema above it changes and the operators are rebuilt
    /// without re-inferring one. Operators that infer no schema of their own reject the inferring path outright.
    return op.withChildrenUnsafe(std::move(children));
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan OriginSplitInsertionRule::apply(const LogicalPlan& queryPlan) const
{
    const auto consumers = countConsumers(queryPlan);

    PlanVisitor<> visitor{
        [&consumers](const LogicalOperator& op, std::vector<LogicalOperator> children) -> PlanVisitor<>::UpResult
        { return headBranchesWithSplits(op, std::move(children), consumers); }};

    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> OriginSplitInsertionRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> OriginSplitInsertionRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType OriginSplitInsertionRule::create(PlanRuleRegistryArguments)
{
    return OriginSplitInsertionRule{};
}
}
