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
#include <string>


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
#include <DataTypes/DataType.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>


namespace NES
{
namespace
{

struct OperatorContext
{   // std::expected?
    std::optional<LogicalFunction> foldedPredicate;
};

using Visitor = PlanVisitor<OperatorContext>;
//[[nodiscard]] PlanVisitor<>::DownResult constantFolding(const LogicalOperator& op/*, const std::vector<std::unordered_set<Field>>& downContexts*/);

DataType chooseDataType(DataType left, DataType right)
{
    // todo: handle diff sizes
    //auto dataType = DataType::inferNumericDataType(left, right);
    // <32 get upped to 32 else use larger, except with float then take float -> DataType.cpp l.38
    if (!(left.isFloat() && right.isFloat())) // if we dont have a float, we use the unsigned data type (see DataType.cpp l.38)
    {
        return (left.isSignedInteger() and right.isInteger()) ? right : left;
    }

    if (left.isFloat() || right.isFloat())
    {
        return left.isFloat() ? left : right;
    }
    return {}; // std::expected? string can be constant value
}

LogicalFunction handleAddition(const std::string left, const std::string right, const DataType dataType)
{
    if (!dataType.isFloat())
    {
        return (dataType.isInteger() ? ConstantValueLogicalFunction(dataType, std::to_string(std::stoul(left) + std::stoul(right)))
            : ConstantValueLogicalFunction(dataType, std::to_string(std::stoi(left) + std::stoi(right))));
    }
    // if not INT or UINT it must be a float
    // warning: if we get more datatypes, this needs to be adjusted!
    return ConstantValueLogicalFunction(dataType, std::to_string(std::stof(left) + std::stof(right)));

}

LogicalFunction calculateExpression(const LogicalFunction& function)
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : function.getChildren())
    {
        newChildren.push_back(calculateExpression(child));
    }
    if (function.tryGetAs<AddLogicalFunction>())
    {
        const auto& left = newChildren.at(0);
        const auto& right = newChildren.at(1);
        if (left.tryGetAs<ConstantValueLogicalFunction>() && right.tryGetAs<ConstantValueLogicalFunction>())
        {
            const auto& leftConstant = left.tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue();
            const auto& rightConstant = right.tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue();
            const auto& leftDataType = left.tryGetAs<ConstantValueLogicalFunction>().value()->getDataType();
            const auto& rightDataType = right.tryGetAs<ConstantValueLogicalFunction>().value()->getDataType();

            const auto dataType = chooseDataType(leftDataType, rightDataType);
            return handleAddition(leftConstant, rightConstant, dataType);
        }
    }
    return function.withChildren(newChildren);
}

Visitor::UpResult rebuildPlan(LogicalOperator op, std::vector<LogicalOperator> children, OperatorContext context) // context: the predicate from the selection
{
    //if (selct and pred then set pred new)
    //{

    //}
    //return op.withChildren(children);
    if (op.tryGetAs<SelectionLogicalOperator>() && context.foldedPredicate)
    {
        LogicalOperator newSelection = SelectionLogicalOperator::create(children.at(0), context.foldedPredicate.value());
        return newSelection;
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

