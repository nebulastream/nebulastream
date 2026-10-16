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

#include <Rules/Static/ConstantFoldingRule.hpp>

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>


#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>


namespace NES
{
namespace
{

struct OperatorContext
{
    std::optional<LogicalFunction> foldedPredicate; // std::optional in case we dont need to fold anything? or maybe throw a check for that earlier?
};

using Visitor = PlanVisitor<OperatorContext>;
//[[nodiscard]] PlanVisitor<>::DownResult constantFolding(const LogicalOperator& op/*, const std::vector<std::unordered_set<Field>>& downContexts*/);


LogicalFunction calculateExpression(/*const LogicalOperator& op*/const LogicalFunction& function)
{
    // check if we have an arithmetic expression in the WHERE-clause
    for (const auto& child : function->getChildren())
    {
        if (const auto& add = child.tryGetAs<AddLogicalFunction>())
        {
            const auto& children = add.value()->getChildren();
            const auto& left = children.at(0);
            const auto& right = children.at(1);
            if (left.tryGetAs<ConstantValueLogicalFunction>() && right.tryGetAs<ConstantValueLogicalFunction>())
            {
                const auto& leftConstant = children.at(0).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue();
                const auto& rightConstant = children.at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue();
                const auto& leftDataType = children.at(0).tryGetAs<ConstantValueLogicalFunction>().value()->getDataType();
                const auto& rightDataType = children.at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getDataType();
                //todo: Can I assume the ConstantValueLogicalFunction only accepts INT and UINT?
                //todo: do i have to check for every U(INT) type when calculating? Is there a rule that both sides need to be the same size? i.e. UINT16, UINT16

            }
        }
        else
        {
            //todo: recursively check for the next children if/until we find a calculation with constants
            calculateExpression(child); // todo: fix, this wont actually propagate anything
        }
    }
    //return op;

} // return something

Visitor::UpResult rebuildPlan(LogicalOperator op, std::vector<LogicalOperator> children, OperatorContext context) // context: the predicate from the selection
{
    auto tmp = op;
    auto tmp2 = children;

    // todo: if we found a predicate to fold, rebuild the plan
    if (context.foldedPredicate) // check if there is a value aka a folded expression
    {

    }

    return op.withChildren(std::move(children));
}

    Visitor::DownResult constantFolding(const LogicalOperator& op/*, const std::vector<std::unordered_set<Field>>& downContexts*/) // todo: should this be a downResult?
{
    //auto tmp = downContexts;
    Visitor::DownResult downResult;


    if (auto selection = op.tryGetAs<SelectionLogicalOperator>())
    {
        auto predicate = selection.value()->getPredicate();

        //const auto& newSelect = calculateExpression(*selection);
        auto newPredicate = calculateExpression(predicate);
        downResult.operatorContext.foldedPredicate = std::move(newPredicate); // store the calcualted predicate in the operator context so we dont lose it (?)
    }
/// return operator or the new logicalFunction?
    return downResult; //op.withChildren(children);
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan ConstantFoldingRule::apply(const LogicalPlan& queryPlan) const
{
    Visitor visitor{constantFolding, rebuildPlan};
    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType ConstantFoldingRule::create(PlanRuleRegistryArguments)
{
    return ConstantFoldingRule{};
}

}

