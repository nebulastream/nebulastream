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

#include <Functions/VarSizedToDoubleLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

VarSizedToDoubleLogicalFunction::VarSizedToDoubleLogicalFunction(const LogicalFunction& child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), child(child)
{
}

DataType VarSizedToDoubleLogicalFunction::getDataType() const
{
    return dataType;
}

VarSizedToDoubleLogicalFunction VarSizedToDoubleLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

LogicalFunction VarSizedToDoubleLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren()
        | std::views::transform([&schema](auto& currentChild) { return currentChild.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();

    INVARIANT(newChildren.size() == 1, "VarSizedToDoubleLogicalFunction expects exactly one child function but has {}", newChildren.size());

    auto newDataType = newChildren[0].getDataType();
    if (newDataType.type != DataType::Type::UNDEFINED && newDataType.type != DataType::Type::VARSIZED)
    {
        throw DifferentFieldTypeExpected("VarSizedToDouble expects a VARSIZED input, but got {}", newDataType);
    }

    newDataType.type = DataType::Type::FLOAT64;
    newDataType.nullable = std::ranges::any_of(newChildren, [](const auto& currentChild) { return currentChild.getDataType().nullable; });

    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> VarSizedToDoubleLogicalFunction::getChildren() const
{
    return {child};
}

VarSizedToDoubleLogicalFunction VarSizedToDoubleLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "VarSizedToDoubleLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view VarSizedToDoubleLogicalFunction::getType() const
{
    return NAME;
}

bool VarSizedToDoubleLogicalFunction::operator==(const VarSizedToDoubleLogicalFunction& rhs) const
{
    return this->dataType == rhs.dataType && this->child == rhs.child;
}

std::string VarSizedToDoubleLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("VarSizedToDoubleLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("VarSizedToDouble({})", child.explain(verbosity));
}

Reflected Reflector<VarSizedToDoubleLogicalFunction>::operator()(const VarSizedToDoubleLogicalFunction& function) const
{
    return reflect(detail::ReflectedVarSizedToDoubleLogicalFunction{.child = function.getChildren()[0]});
}

VarSizedToDoubleLogicalFunction Unreflector<VarSizedToDoubleLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [childFunction] = unreflect<detail::ReflectedVarSizedToDoubleLogicalFunction>(reflected);
    return VarSizedToDoubleLogicalFunction(childFunction);
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterVarSizedToDoubleLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<VarSizedToDoubleLogicalFunction>(arguments.reflected);
    }

    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("VarSizedToDoubleLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }

    return VarSizedToDoubleLogicalFunction(arguments.children.front());
}

}
