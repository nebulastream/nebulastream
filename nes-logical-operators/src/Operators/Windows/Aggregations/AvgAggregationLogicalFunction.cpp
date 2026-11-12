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

#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/AggregationScalarFunctions.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <magic_enum/magic_enum.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(AggregationFieldAccess inputFunction) : inputFunction(std::move(inputFunction))
{
}

DataType AvgAggregationLogicalFunction::getAggregateType() const
{
    return aggregateType;
}

bool AvgAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

AggregationFieldAccess AvgAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

LogicalScalarFunctions AvgAggregationLogicalFunction::getScalarFunctions() const
{
    return scalarFunctions;
}

LogicalScalarFunctions AvgAggregationLogicalFunction::buildScalarFunctions(const DataType& inputType, const DataType& resultType)
{
    using namespace AggregationScalarFunctionRoles;
    using namespace AggregationOperands;
    const auto countType = DataTypeProvider::provideDataType(DataType::Type::UINT64);
    /// The running sum is kept in the result type. lift: sum = sum + input, combine: sum = sum + otherSum, lower: sum / count.
    /// The count is deliberately not cast to the result type: a type that supports AVG has to support being divided by an
    /// integer count, otherwise the aggregation is not supported for it.
    return {
        {std::string{LIFT},
         castUnlessStruct(resultType, AddLogicalFunction{aggregationOperand(STATE, resultType), aggregationOperand(INPUT, inputType)})},
        {std::string{COMBINE},
         castUnlessStruct(
             resultType, AddLogicalFunction{aggregationOperand(STATE, resultType), aggregationOperand(OTHER_STATE, resultType)})},
        {std::string{LOWER}, DivLogicalFunction{aggregationOperand(STATE, resultType), aggregationOperand(COUNT, countType)}},
    };
}

std::string_view AvgAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

std::string AvgAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool AvgAggregationLogicalFunction::operator==(const AvgAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

AvgAggregationLogicalFunction AvgAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    const auto newInputFunction = inferFieldAccess(inputFunction, schema);
    const auto& inputType = newInputFunction->getDataType();
    /// User-defined (STRUCT) types are averaged in their own type. Whether the type supports being added and divided by the count
    /// is verified by the type inference of the scalar functions (Add_<Type>_<Type> and Div_<Type>_UINT64 have to be provided
    /// by the type's plugin).
    if (!inputType.isNumeric() and inputType.type != DataType::Type::STRUCT)
    {
        throw UnsupportedQuery(
            "{} is only supported on numeric fields, but field {} has type {}",
            NAME,
            newInputFunction->getField().getLastName(),
            magic_enum::enum_name(newInputFunction->getDataType().type));
    }
    auto newAggFunction = AvgAggregationLogicalFunction{newInputFunction};
    newAggFunction.aggregateType = inputType.type == DataType::Type::STRUCT
        ? inputType
        : DataTypeProvider::provideDataType(
              finalAggregateStampType, inputType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    newAggFunction.scalarFunctions
        = inferScalarFunctions(buildScalarFunctions(newInputFunction->getDataType(), newAggFunction.getAggregateType()), schema);
    return newAggFunction;
}

Reflected
Reflector<AvgAggregationLogicalFunction>::operator()(const AvgAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(function.getInputFunction());
}

AvgAggregationLogicalFunction
Unreflector<AvgAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return AvgAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType AvgAggregationLogicalFunction::create(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("AvgAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return AvgAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t
std::hash<NES::AvgAggregationLogicalFunction>::operator()(const NES::AvgAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), NES::AvgAggregationLogicalFunction::getName());
}
