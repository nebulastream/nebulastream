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
#include <Phases/DecideJoinTypes.hpp>

#include <algorithm>
#include <ranges>
#include <unordered_set>
#include <vector>

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/ImplementationTypeTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

namespace
{
/// We set the join type to be a Hash-Join if the join function consists of solely functions that are FieldAccessLogicalFunction.
/// To be more specific, we currently support only
/// Otherwise, we use a NLJ.
bool shallUseHashJoin(const LogicalFunction& joinFunction)
{
    /// Checks if the logical function is allowed to be in our join function for a hash join
    auto allowedLogicalFunction = [](const LogicalFunction& logicalFunction)
    {
        return logicalFunction.tryGetAs<AndLogicalFunction>().has_value() or logicalFunction.tryGetAs<EqualsLogicalFunction>().has_value()
            or logicalFunction.tryGetAs<OrLogicalFunction>().has_value()
            or logicalFunction.tryGetAs<FieldAccessLogicalFunction>().has_value();
    };

    std::unordered_set<LogicalFunction> parentsOfJoinComparisons;
    for (auto logicalFunction : BFSRange<LogicalFunction>(joinFunction))
    {
        if (not allowedLogicalFunction(logicalFunction))
        {
            return false;
        }

        const auto anyChildIsLeaf
            = std::ranges::any_of(logicalFunction.getChildren(), [](const LogicalFunction& child) { return child.getChildren().empty(); });
        if (anyChildIsLeaf)
        {
            for (const auto& child : logicalFunction.getChildren())
            {
                if (not child.tryGetAs<FieldAccessLogicalFunction>().has_value())
                {
                    /// If the leaf is not a FieldAccessLogicalFunction, we need to use a NLJ
                    return false;
                }
            }
        }
    }

    return true;
}
}

LogicalPlan DecideJoinTypes::apply(const LogicalPlan& queryPlan)
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Only single root operators are supported for now");
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    return LogicalPlan{queryPlan.getQueryId(), {apply(queryPlan.getRootOperators()[0])}};
}

LogicalOperator DecideJoinTypes::apply(const LogicalOperator& logicalOperator)
{
    const auto children = logicalOperator.getChildren()
        | std::views::transform([this](const LogicalOperator& child) { return apply(child); }) | std::ranges::to<std::vector>();
    auto traitSet = logicalOperator.getTraitSet();
    if (const auto joinOperator = logicalOperator.tryGetAs<JoinLogicalOperator>())
    {
        if (this->joinStrategy == StreamJoinStrategy::NESTED_LOOP_JOIN)
        {
            tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});
        }
        else if (shallUseHashJoin(joinOperator.value()->getJoinFunction()))
        {
            tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});
        }
        else
        {
            tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});
            if (this->joinStrategy == StreamJoinStrategy::HASH_JOIN)
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
