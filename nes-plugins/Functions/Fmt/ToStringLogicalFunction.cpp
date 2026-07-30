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

#include "ToStringLogicalFunction.hpp"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

ToStringLogicalFunction::ToStringLogicalFunction(LogicalFunction child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), child(std::move(child))
{
}

DataType ToStringLogicalFunction::getDataType() const
{
    return dataType;
}

ToStringLogicalFunction ToStringLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction ToStringLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    if (newChild.getDataType().type == DataType::Type::UNDEFINED)
    {
        throw CannotInferSchema("to_string requires a typed argument, but the child resolved to UNDEFINED");
    }
    auto outputType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    outputType.nullable = newChild.getDataType().nullable;
    return withDataType(outputType).withChildren({newChild});
}

std::vector<LogicalFunction> ToStringLogicalFunction::getChildren() const
{
    return {child};
}

ToStringLogicalFunction ToStringLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "to_string requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view ToStringLogicalFunction::getType() const
{
    return NAME;
}

bool ToStringLogicalFunction::operator==(const ToStringLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string ToStringLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("to_string({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("to_string({})", child.explain(verbosity));
}

Reflected Reflector<ToStringLogicalFunction>::operator()(const ToStringLogicalFunction& function) const
{
    return reflect(detail::ReflectedToStringLogicalFunction{.child = function.child});
}

ToStringLogicalFunction Unreflector<ToStringLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedToStringLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("ToStringLogicalFunction is missing its child");
    }
    return ToStringLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::Registerto_stringLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ToStringLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("to_string requires exactly one child, but got {}", arguments.children.size());
    }
    return ToStringLogicalFunction(arguments.children.front());
}

}
