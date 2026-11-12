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

#include <Functions/AggregationOperandLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

AggregationOperandLogicalFunction::AggregationOperandLogicalFunction(std::string operandName, DataType dataType)
    : operandName(std::move(operandName)), dataType(std::move(dataType))
{
}

std::string AggregationOperandLogicalFunction::getOperandName() const
{
    return operandName;
}

DataType AggregationOperandLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction AggregationOperandLogicalFunction::withInferredDataType(const Schema<Field, Unordered>&) const
{
    /// The operand is not part of any schema; its type is fixed by the aggregation function that created it.
    return *this;
}

std::vector<LogicalFunction> AggregationOperandLogicalFunction::getChildren() const
{
    return {};
}

AggregationOperandLogicalFunction AggregationOperandLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
}

std::string_view AggregationOperandLogicalFunction::getType() const
{
    return NAME;
}

bool AggregationOperandLogicalFunction::operator==(const AggregationOperandLogicalFunction& rhs) const
{
    return operandName == rhs.operandName and dataType == rhs.dataType;
}

std::string AggregationOperandLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("AggregationOperandLogicalFunction({} : {})", operandName, dataType);
    }
    return operandName;
}

Reflected Reflector<AggregationOperandLogicalFunction>::operator()(
    const AggregationOperandLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedAggregationOperandLogicalFunction{.operandName = function.getOperandName(), .dataType = function.getDataType()});
}

AggregationOperandLogicalFunction
Unreflector<AggregationOperandLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [operandName, dataType] = context.unreflect<detail::ReflectedAggregationOperandLogicalFunction>(reflected);
    return AggregationOperandLogicalFunction{std::move(operandName), std::move(dataType)};
}

/// NOLINTBEGIN(performance-unnecessary-value-param)
LogicalFunctionRegistryReturnType AggregationOperandLogicalFunction::createAggregationOperand(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "Function is only built directly by aggregation functions or via reflection, not using the registry");
    std::unreachable();
}

/// NOLINTEND(performance-unnecessary-value-param)
}
