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
#include <Rules/Static/DecideJoinTypesRule.hpp>

#include <set>
#include <typeindex>
#include <typeinfo>
#include <vector>

#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Windows/JoinFunctionSplit.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Traits/JoinImplementationTypeTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

namespace
{
/// We use a hash join if at least one top-level conjunct of the join function is an equi-join key `left.field = right.field`.
/// The hash join hashes on these keys and evaluates the remaining conjuncts (the residual) on every candidate pair.
/// A join function without such a conjunct (pure theta join, top-level OR) needs a NLJ.
bool shallUseHashJoin(const JoinLogicalOperator& joinOperator)
{
    return not splitJoinFunction(joinOperator).equiKeys.empty();
}

bool isProducedBy(const Field& field, const LogicalOperator& input)
{
    return std::ranges::any_of(BFSRange<LogicalOperator>(input), [&](const auto& producer) { return producer == field.getProducedBy(); });
}

bool shallUseIndexNestedLoopJoin(const LogicalFunction& joinFunction, const LogicalOperator& leftInput, const LogicalOperator& rightInput)
{
    const auto isSupportedComparison = joinFunction.tryGetAs<LessLogicalFunction>().has_value()
        or joinFunction.tryGetAs<LessEqualsLogicalFunction>().has_value() or joinFunction.tryGetAs<GreaterLogicalFunction>().has_value()
        or joinFunction.tryGetAs<GreaterEqualsLogicalFunction>().has_value();
    if (not isSupportedComparison)
    {
        return false;
    }
    const auto children = joinFunction.getChildren();
    if (children.size() != 2)
    {
        return false;
    }
    const auto lhs = children[0].tryGetAs<FieldAccessLogicalFunction>();
    const auto rhs = children[1].tryGetAs<FieldAccessLogicalFunction>();
    if (not lhs.has_value() or not rhs.has_value())
    {
        return false;
    }
    const auto lhsType = lhs->getDataType();
    const auto rhsType = rhs->getDataType();
    const auto lhsField = lhs.value()->getField();
    const auto rhsField = rhs.value()->getField();
    const auto comparesInputs = (isProducedBy(lhsField, leftInput) and isProducedBy(rhsField, rightInput))
        or (isProducedBy(lhsField, rightInput) and isProducedBy(rhsField, leftInput));
    return lhsType == rhsType and not lhsType.nullable and lhsType.isNumeric() and comparesInputs;
}

LogicalOperator
decideJoinTypes(const LogicalOperator& logicalOperator, const std::vector<LogicalOperator>& children, const StreamJoinStrategy joinStrategy)
{
    auto traitSet = logicalOperator.getTraitSet();
    if (const auto joinOperator = logicalOperator.tryGetAs<JoinLogicalOperator>())
    {
        if (joinStrategy == StreamJoinStrategy::NESTED_LOOP_JOIN)
        {
            tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});
        }
        else if (shallUseHashJoin(*joinOperator.value()))
        {
            tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});
        }
        else if (shallUseIndexNestedLoopJoin(
                     joinOperator.value()->getJoinFunction(),
                     joinOperator.value()->getBothChildren()[0],
                     joinOperator.value()->getBothChildren()[1]))
        {
            tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::INDEX_NESTED_LOOP_JOIN});
        }
        else
        {
            tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});
            if (joinStrategy == StreamJoinStrategy::HASH_JOIN)
            {
                NES_WARNING(
                    "Operator {} has not the HashJoinTrait, as the hash join is not supported for the join condition. Therefore, we "
                    "fall-back to the NLJ!",
                    logicalOperator);
            }
        }
    }
    else
    {
        tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::CHOICELESS});
    }
    return logicalOperator.withChildren(children).withTraitSet(traitSet);
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideJoinTypesRule::needs() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

LogicalPlan DecideJoinTypesRule::apply(const LogicalPlan& queryPlan) const
{
    PlanVisitor<> visitor{
        [this](const LogicalOperator& op, const std::vector<LogicalOperator>& children) -> PlanVisitor<>::UpResult
        { return decideJoinTypes(op, children, this->joinStrategy); }};

    return visitor.apply(queryPlan);
}

bool DecideJoinTypesRule::operator==(const DecideJoinTypesRule& other) const
{
    return this->joinStrategy == other.joinStrategy;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType DecideJoinTypesRule::create(PlanRuleRegistryArguments arguments)
{
    return DecideJoinTypesRule{arguments.defaultQueryOptimization.joinStrategy};
}
}
