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
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>


namespace NES
{
namespace
{

LogicalOperator calculateExpression(const LogicalOperator& op)
{
    // check if we have an arithmetic expression in the WHERE-clause
    for (const auto& child : op.getChildren())
    {
        if (const auto& add = child.tryGetAs<AddLogicalFunction>())
        {
            // todo: do something and return result
        }
        else
        {
            //todo: recursively check for the next children if/until we find a calculation with constants
            calculateExpression(child); // todo: fix, this wont actually propagate anything
        }
    }
    return op;
}

PlanVisitor<>::UpResult constantFolding(const LogicalOperator& op, const std::vector<LogicalOperator>& children) // todo: should this be a downResult?
{
    auto tmp = children; //todo: rm
    //Visitor::DownResult downResult;

    if (const auto selection = op.tryGetAs<SelectionLogicalOperator>())
    {
        const auto& newSelect = calculateExpression(*selection);
    }
//todo: return operator or the new logicalFunction
    return op.withChildren(children);
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan ConstantFoldingRule::apply(const LogicalPlan& queryPlan) const
{
    PlanVisitor<> visitor{constantFolding/*, rebuildPlan*/}; // todo: rebuild plan function
    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType ConstantFoldingRule::create(PlanRuleRegistryArguments)
{
    return ConstantFoldingRule{};
}

}

