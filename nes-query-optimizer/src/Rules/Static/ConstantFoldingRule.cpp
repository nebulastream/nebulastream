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
#include <iostream>

#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/PlanVisitor.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/PowLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/AbsoluteLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/CeilLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ExpLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/FloorLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/RoundLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SqrtLogicalFunction.hpp>
#include <cmath>

#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>

namespace NES
{
namespace
{

struct OperatorContext
{
    std::optional<LogicalFunction> foldedPredicate;
};

using Visitor = PlanVisitor<OperatorContext>;

LogicalFunction handleAddition(const std::string left, const std::string right, const DataType dataType)
{
    if (!dataType.isFloat())
    {
        return (dataType.isInteger() ? ConstantValueLogicalFunction(dataType, std::to_string(std::stoul(left) + std::stoul(right)))
            : ConstantValueLogicalFunction(dataType, std::to_string(std::stoi(left) + std::stoi(right))));
    }
    // if not INT or UINT it must be a float
    return ConstantValueLogicalFunction(dataType, std::to_string(std::stof(left) + std::stof(right)));

}

LogicalFunction handleMultiplication(const std::string left, const std::string right, const DataType dataType)
{
    if (!dataType.isFloat())
    {
        return (dataType.isInteger() ? ConstantValueLogicalFunction(dataType, std::to_string(std::stoul(left) * std::stoul(right)))
            : ConstantValueLogicalFunction(dataType, std::to_string(std::stoi(left) * std::stoi(right))));
    }
    return ConstantValueLogicalFunction(dataType, std::to_string(std::stof(left) * std::stof(right)));
}

LogicalFunction handleDivision(const std::string left, const std::string right, const DataType dataType)
{
    if (left == "0" || right == "0")
    {
        throw InvalidQuerySyntax("Division by null!");
    }
    if (!dataType.isFloat())
    {
        return (dataType.isInteger() ? ConstantValueLogicalFunction(dataType, std::to_string(std::stoul(left) / std::stoul(right)))
            : ConstantValueLogicalFunction(dataType, std::to_string(std::stoi(left) / std::stoi(right))));
    }
    return ConstantValueLogicalFunction(dataType, std::to_string(std::stof(left) / std::stof(right)));
}

LogicalFunction handleSubtraction(const std::string left, const std::string right, const DataType dataType)
{
    if (!dataType.isFloat())
    {
        return (dataType.isInteger() ? ConstantValueLogicalFunction(dataType, std::to_string(std::stoul(left) - std::stoul(right)))
            : ConstantValueLogicalFunction(dataType, std::to_string(std::stoi(left) - std::stoi(right))));
    }
    return ConstantValueLogicalFunction(dataType, std::to_string(std::stof(left) - std::stof(right)));

}

LogicalFunction handleModulo(const std::string left, const std::string right, const DataType dataType)
{
    if (dataType.isFloat())
    {
        throw InvalidQuerySyntax("Modulo cant be applied to floats!");
    }
    if (dataType.isInteger())
    {
        return ConstantValueLogicalFunction(dataType, std::to_string(std::stoul(left) % std::stoul(right)));
    }
    return ConstantValueLogicalFunction(dataType, std::to_string(std::stoi(left) % std::stoi(right)));
}

LogicalFunction handlePow(const std::string left, const std::string right, const DataType dataType)
{
    if (!dataType.isFloat())
    {
        if (dataType.isSignedInteger())
        {
            auto res = 1;
            for (auto i = 0; i < std::stoi(right); i++)
            {
                res = res*std::stoi(left);
            }
            return ConstantValueLogicalFunction(dataType, std::to_string(res));
        }
        if (dataType.isInteger())
        {
            unsigned long res = 1;
            auto exp = std::stoul(right);
            for (unsigned long i = 0; i < exp; i++)
            {
                res = res*std::stoul(left);
            }
            return ConstantValueLogicalFunction(dataType, std::to_string(res));
        }
    }
    auto res = std::pow(std::stof(left), std::stof(right));
    return ConstantValueLogicalFunction(dataType, std::to_string(res));
}

LogicalFunction handleAbsolute(const std::string value, const DataType dataType)
{
    if (value.starts_with("-"))
    {
        NES_DEBUG("value is negative");
        auto absoluteValueAsString = value.substr(1, value.size() - 1);
        NES_DEBUG("absoluteValueAsString: {}", absoluteValueAsString);
        auto tmp = ConstantValueLogicalFunction(dataType, absoluteValueAsString);
        NES_DEBUG("created Function: {}", tmp.explain(ExplainVerbosity::Debug));
        return ConstantValueLogicalFunction(dataType, absoluteValueAsString);
    }
    NES_DEBUG("return with value: {}", value);
    auto tmp = ConstantValueLogicalFunction(dataType, value);
    NES_DEBUG("created Function: {}", tmp.explain(ExplainVerbosity::Debug));
    return ConstantValueLogicalFunction(dataType, value);
}

LogicalFunction handleFloor(const std::string value, const DataType dataType)
{
    std::string newValue = std::to_string(std::stoi(value));
    if (value.starts_with("-"))
    {
        newValue = std::to_string(std::stoi(value) -1);
    }
    return ConstantValueLogicalFunction(dataType, newValue);
}

LogicalFunction handleRound(const std::string value, const DataType dataType)
{
    const std::string newValue = std::to_string(std::round(std::stof(value)));
    return ConstantValueLogicalFunction(dataType, newValue);
}

LogicalFunction handleSqrt(const std::string value, const DataType dataType)
{
    const std::string newValue = std::to_string(std::sqrt(std::stof(value)));
    return ConstantValueLogicalFunction(dataType, newValue);
}

LogicalFunction handleExp(const std::string value, const DataType dataType)
{
    const std::string eulerNumber = "2.7182818284590452353602874713527";
    //const auto newValue = handlePow(eulerNumber, value, dataType);
    return handlePow(eulerNumber, value, dataType);
}

LogicalFunction handleCeil(const std::string value, const DataType dataType)
{
    const std::string newValue = std::to_string(std::ceil(std::stof(value)));
    return ConstantValueLogicalFunction(dataType, newValue);
}

std::optional<std::string> getInputConstantValue(const LogicalFunction& function)
{
    if (function.tryGetAs<ConstantValueLogicalFunction>())
    {
        const auto constantValue = function.tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue();
        return constantValue;
    }
    return {};
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
        const auto& left = getInputConstantValue(newChildren.at(0));
        const auto& right = getInputConstantValue(newChildren.at(1));
        if (left.has_value() && right.has_value())
        {
            DataType dataType;
            auto newDataType = dataType.inferNumericDataType(
            newChildren.at(0).getAs<ConstantValueLogicalFunction>().getDataType(), newChildren.at(1).getAs<ConstantValueLogicalFunction>().getDataType());
            return handleAddition(left.value(), right.value(), newDataType.value());
        }
    }
    if (function.tryGetAs<MulLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        const auto& right = getInputConstantValue(newChildren.at(1));
        if (left.has_value() && right.has_value())
        {
            DataType dataType;
            auto newDataType = dataType.inferNumericDataType(
            newChildren.at(0).getAs<ConstantValueLogicalFunction>().getDataType(), newChildren.at(1).getAs<ConstantValueLogicalFunction>().getDataType());
            return handleMultiplication(left.value(), right.value(), newDataType.value());
        }
    }
    if (function.tryGetAs<DivLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        const auto& right = getInputConstantValue(newChildren.at(1));
        if (left.has_value() && right.has_value())
        {
            DataType dataType;
            auto newDataType = dataType.inferNumericDataType(
            newChildren.at(0).getAs<ConstantValueLogicalFunction>().getDataType(), newChildren.at(1).getAs<ConstantValueLogicalFunction>().getDataType());
            return handleDivision(left.value(), right.value(), newDataType.value());
        }
    }
    if (function.tryGetAs<SubLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        const auto& right = getInputConstantValue(newChildren.at(1));
        if (left.has_value() && right.has_value())
        {
            DataType dataType;
            auto newDataType = dataType.inferNumericDataType(
            newChildren.at(0).getAs<ConstantValueLogicalFunction>().getDataType(), newChildren.at(1).getAs<ConstantValueLogicalFunction>().getDataType());
            return handleSubtraction(left.value(), right.value(), newDataType.value());
        }
    }
    if (function.tryGetAs<ModuloLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        const auto& right = getInputConstantValue(newChildren.at(1));
        if (left.has_value() && right.has_value())
        {
            DataType dataType;
            auto newDataType = dataType.inferNumericDataType(
            newChildren.at(0).getAs<ConstantValueLogicalFunction>().getDataType(), newChildren.at(1).getAs<ConstantValueLogicalFunction>().getDataType());
            return handleModulo(left.value(), right.value(), newDataType.value());
        }
    }
    if (function.tryGetAs<PowLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        const auto& right = getInputConstantValue(newChildren.at(1));
        if (left.has_value() && right.has_value())
        {
            DataType dataType;
            auto newDataType = dataType.inferNumericDataType(
            newChildren.at(0).getAs<ConstantValueLogicalFunction>().getDataType(), newChildren.at(1).getAs<ConstantValueLogicalFunction>().getDataType());
            return handlePow(left.value(), right.value(), newDataType.value());
        }
    }
    if (function.tryGetAs<AbsoluteLogicalFunction>())
    {
        NES_DEBUG("function is ABS");
        const auto& left = getInputConstantValue(newChildren.at(0));
        if (left.has_value())
        {
            DataType dataType;
            if (!function.getAs<AbsoluteLogicalFunction>().getChildren().at(0).tryGetAs<ConstantValueLogicalFunction>()->getDataType().isSignedInteger())
            {
                NES_DEBUG("function is NOT signed");
                dataType = function.getAs<AbsoluteLogicalFunction>().getChildren().at(0).tryGetAs<ConstantValueLogicalFunction>()->getDataType();
                NES_DEBUG("function: {}", function.getChildren().at(0).explain(ExplainVerbosity::Debug));
                return handleAbsolute(left.value(), dataType);
            }
            NES_DEBUG("function IS signed");
            const auto childType = function.getAs<AbsoluteLogicalFunction>().getChildren().at(0).tryGetAs<ConstantValueLogicalFunction>()->getDataType().type;
            const auto absInferredType = function.getAs<AbsoluteLogicalFunction>()->inferAbsoluteResultType(childType);
            NES_DEBUG("function: {}", function.getChildren().at(0).explain(ExplainVerbosity::Debug));
            if (newChildren.at(0).getAs<ConstantValueLogicalFunction>().getDataType().nullable)
            {
                dataType = DataType{absInferredType, DataType::NULLABLE::IS_NULLABLE};
            }
            else
            {
                dataType = DataType{absInferredType, DataType::NULLABLE::NOT_NULLABLE};
            }
            return handleAbsolute(left.value(), dataType);
        }
    }
    if (function.tryGetAs<FloorLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        if (left.has_value() && newChildren.at(0).getDataType().isFloat())
        {
            return handleFloor(left.value(), newChildren.at(0).getDataType());
        }
    }
    if (function.tryGetAs<RoundLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        if (left.has_value() && newChildren.at(0).getDataType().isFloat())
        {
            return handleRound(left.value(), newChildren.at(0).getDataType());
        }
    }
    if (function.tryGetAs<SqrtLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        if (left.has_value())
        {
            const auto dataType = DataTypeProvider::provideDataType(
            DataType::Type::FLOAT64, newChildren.at(0).getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
            return handleSqrt(left.value(), dataType);
        }
    }
    if (function.tryGetAs<ExpLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        if (left.has_value())
        {
            const auto dataType = DataTypeProvider::provideDataType(
            DataType::Type::FLOAT64, newChildren.at(0).getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
            return handleExp(left.value(), dataType);
        }
    }
    if (function.tryGetAs<CeilLogicalFunction>())
    {
        const auto& left = getInputConstantValue(newChildren.at(0));
        if (left.has_value() && newChildren.at(0).getDataType().isFloat())
        {
            return handleCeil(left.value(), newChildren.at(0).getDataType());
        }
    }


    return function.withChildren(newChildren);
}

Visitor::UpResult rebuildPlan(LogicalOperator op, std::vector<LogicalOperator> children, OperatorContext context) // context: the predicate from the selection
{
    if (op.tryGetAs<SelectionLogicalOperator>() && context.foldedPredicate)
    {
        LogicalOperator newSelection = SelectionLogicalOperator::create(children.at(0), context.foldedPredicate.value());
        return newSelection;
    }
    return op.withChildren(children);
}

Visitor::DownResult constantFolding(const LogicalOperator& op)
{
    Visitor::DownResult downResult;

    if (auto selection = op.tryGetAs<SelectionLogicalOperator>())
    {
        auto predicate = selection.value()->getPredicate();
        auto newPredicate = calculateExpression(predicate);
        downResult.operatorContext.foldedPredicate = std::move(newPredicate);
    }
    return downResult;
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan ConstantFoldingRule::apply(const LogicalPlan& queryPlan) const
{
    NES_DEBUG("in apply");
    Visitor visitor{constantFolding, rebuildPlan};
    return visitor.apply(queryPlan);
}

std::set<std::type_index> ConstantFoldingRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

std::set<std::type_index> ConstantFoldingRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType ConstantFoldingRule::create(PlanRuleRegistryArguments)
{
    return ConstantFoldingRule{};
}

}
