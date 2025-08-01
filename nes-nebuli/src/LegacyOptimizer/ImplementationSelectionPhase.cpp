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

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Iterators/DFSIterator.hpp>
#include <LegacyOptimizer/ImplementationSelectionPhase.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Traits/ImplementationTrait.hpp>


namespace NES::LegacyOptimizer
{

namespace
{
/// We set the join type to be a Hash-Join if the join function consists of solely functions that are FieldAccessLogicalFunction.
/// To be more specific, we currently support only
/// Otherwise, we use a NLJ.
bool canUseHashJoin(const LogicalFunction& joinFunction)
{
    /// Checks if the logical function is allowed to be in our join function for a hash join
    auto allowedLogicalFunction = [](const LogicalFunction& logicalFunction)
    {
        return logicalFunction.tryGet<AndLogicalFunction>().has_value() or logicalFunction.tryGet<EqualsLogicalFunction>().has_value()
            or logicalFunction.tryGet<OrLogicalFunction>().has_value() or logicalFunction.tryGet<GreaterEqualsLogicalFunction>().has_value()
            or logicalFunction.tryGet<GreaterLogicalFunction>().has_value()
            or logicalFunction.tryGet<LessEqualsLogicalFunction>().has_value() or logicalFunction.tryGet<LessLogicalFunction>().has_value();
    };

    std::unordered_set<LogicalFunction> parentsOfJoinComparisons;
    for (auto itr : BFSRange<LogicalFunction>(joinFunction))
    {
        /// If any child is a leaf function, we put the current function into the set
        const auto anyChildIsLeaf
            = std::ranges::any_of(itr.getChildren(), [](const LogicalFunction& child) { return child.getChildren().empty(); });

        if (not allowedLogicalFunction(itr))
        {
            return false;
        }

        if (anyChildIsLeaf)
        {
            for (const auto& child : itr.getChildren())
            {
                if (child.tryGet<FieldAccessLogicalFunction>().has_value())
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

void ImplementationSelectionPhase::apply(LogicalPlan& queryPlan) const
{
    for (auto logicalOperator : DFSRange(queryPlan.getRootOperators().front()))
    {
        auto traits = logicalOperator.getTraitSet();

        if (auto joinOp = logicalOperator.tryGet<JoinLogicalOperator>())
        {
            if ((configuration.joinStrategy.getValue() == StreamJoinStrategy::HASH_JOIN
                 || configuration.joinStrategy.getValue() == StreamJoinStrategy::OPTIMIZER_CHOOSES)
                && canUseHashJoin(joinOp->getJoinFunction()))
            {
                traits = addTrait<ImplementationTrait>(std::move(traits), "HashJoin");
            }
            else
            {
                if (configuration.joinStrategy.getValue() == StreamJoinStrategy::HASH_JOIN)
                {
                    NES_WARNING(
                        "Hash Join was requested, but is impossible. Choosing Nested Loop Join.\n{}",
                        joinOp->explain(ExplainVerbosity::Short));
                }
                traits = addTrait<ImplementationTrait>(std::move(traits), "NLJoin");
            }
        }
        else
        {
            traits = addTrait<ImplementationTrait>(std::move(traits), std::string(logicalOperator.getName()));
        }
        queryPlan = replaceOperator(queryPlan, logicalOperator.getId(), logicalOperator.withTraitSet(traits)).value();
    }
}

}
