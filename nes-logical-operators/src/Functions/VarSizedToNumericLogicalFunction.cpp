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

#include <Functions/VarSizedToNumericLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

VarSizedToNumericLogicalFunction::VarSizedToNumericLogicalFunction(LogicalFunction child, const DataType& targetType)
    : targetType(targetType), child(std::move(child))
{
}

DataType VarSizedToNumericLogicalFunction::getDataType() const
{
    return targetType;
}

VarSizedToNumericLogicalFunction VarSizedToNumericLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.targetType = newDataType;
    return copy;
}

bool VarSizedToNumericLogicalFunction::isSupportedNumericType(const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
            return true;
        default:
            return false;
    }
}

LogicalFunction VarSizedToNumericLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.child = child.withInferredDataType(schema);
    if (copy.child.getDataType().type != DataType::Type::UNDEFINED && copy.child.getDataType().type != DataType::Type::VARSIZED)
    {
        throw DifferentFieldTypeExpected("VarSizedToNumeric expects a VARSIZED input, but got {}", copy.child.getDataType());
    }

    if (!isSupportedNumericType(targetType.type))
    {
        throw DifferentFieldTypeExpected("VarSizedToNumeric expects a numeric target type, but got {}", targetType);
    }

    auto inferredOutputType = targetType;
    inferredOutputType.nullable = copy.child.getDataType().nullable;
    return copy.withDataType(inferredOutputType);
}

std::vector<LogicalFunction> VarSizedToNumericLogicalFunction::getChildren() const
{
    return {child};
}

VarSizedToNumericLogicalFunction VarSizedToNumericLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "VarSizedToNumericLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view VarSizedToNumericLogicalFunction::getType() const
{
    return NAME;
}

bool VarSizedToNumericLogicalFunction::operator==(const VarSizedToNumericLogicalFunction& rhs) const
{
    return this->targetType == rhs.targetType && this->targetType == rhs.targetType && this->child == rhs.child;
}

std::string VarSizedToNumericLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("VarSizedToNumericLogicalFunction({} -> {}, targetType={})", child.explain(verbosity), targetType, targetType);
    }

    return fmt::format("VarSizedToNumeric({}, {})", child.explain(verbosity), targetType);
}

Reflected Reflector<VarSizedToNumericLogicalFunction>::operator()(
    const VarSizedToNumericLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedVarSizedToNumericLogicalFunction{
        .child = function.getChildren()[0],
        .targetType = function.getDataType(),
    });
}

VarSizedToNumericLogicalFunction
Unreflector<VarSizedToNumericLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [childFunction, targetType] = context.unreflect<detail::ReflectedVarSizedToNumericLogicalFunction>(reflected);
    return VarSizedToNumericLogicalFunction{childFunction, targetType};
}

/// NOLINTBEGIN(performance-unnecessary-value-param)
LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterVarSizedToNumericLogicalFunction(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "Function is only built directly via parser or via reflection, not using the registry");
    std::unreachable();
}

/// NOLINTEND(performance-unnecessary-value-param)

}
