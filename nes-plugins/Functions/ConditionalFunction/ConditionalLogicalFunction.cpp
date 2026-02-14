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

#include "ConditionalLogicalFunction.hpp"

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
ConditionalLogicalFunction::ConditionalLogicalFunction(std::vector<LogicalFunction> children)
    : dataType(children.back().getDataType()), children(std::move(children))
{
}

DataType ConditionalLogicalFunction::getDataType() const
{
    return dataType;
}

ConditionalLogicalFunction ConditionalLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

std::vector<LogicalFunction> ConditionalLogicalFunction::getChildren() const
{
    return children;
}

ConditionalLogicalFunction ConditionalLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    return ConditionalLogicalFunction(children);
}

std::string_view ConditionalLogicalFunction::getType() const
{
    return NAME;
}

bool ConditionalLogicalFunction::operator==(const ConditionalLogicalFunction& rhs) const
{
    return children == rhs.children;
}

std::string ConditionalLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string result = "CASE";
    for (size_t i = 0; i + 1 < children.size(); i += 2)
    {
        result += fmt::format(" WHEN {} THEN {}", children.at(i).explain(verbosity), children.at(i + 1).explain(verbosity));
    }
    result += fmt::format(" ELSE {}", children.back().explain(verbosity));
    return result;
}

LogicalFunction ConditionalLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> inferredChildren;
    inferredChildren.reserve(children.size());
    for (const auto& child : children)
    {
        inferredChildren.push_back(child.withInferredDataType(schema));
    }

    /// Even indexes (0, 2, 4, ...) are conditions and must be BOOLEAN.
    for (std::size_t i = 0; i + 1 < inferredChildren.size(); i += 2)
    {
        PRECONDITION(
            inferredChildren.at(i).getDataType().isType(DataType::Type::BOOLEAN),
            "ConditionalLogicalFunction: condition at index {} must be BOOLEAN, but was: {}",
            i,
            inferredChildren.at(i).getDataType());
    }

    /// Odd indexes (1, 3, 5, ...) are results and must have the same type as the default (last element).
    const auto& resultType = inferredChildren.back().getDataType();
    for (std::size_t i = 1; i + 1 < inferredChildren.size(); i += 2)
    {
        PRECONDITION(
            inferredChildren.at(i).getDataType() == resultType,
            "ConditionalLogicalFunction: result at index {} has type {}, but expected {} (matching default)",
            i,
            inferredChildren.at(i).getDataType(),
            resultType);
    }

    return withChildren(inferredChildren).withDataType(resultType);
}

SerializableFunction ConditionalLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    for (const auto& child : children)
    {
        serializedFunction.add_children()->CopyFrom(child.serialize());
    }
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConditionalLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    /// Must have an odd number of children >= 3: at least one (condition, result) pair + default.
    if (arguments.children.size() < 3 || arguments.children.size() % 2 == 0)
    {
        throw CannotDeserialize(
            "ConditionalLogicalFunction requires an odd number of arguments >= 3 "
            "(condition/result pairs + default), but got {}",
            arguments.children.size());
    }
    return ConditionalLogicalFunction(std::move(arguments.children));
}
}
