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
#include <Functions/BooleanFunctions/IsNaNLogicalFunction.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

IsNaNLogicalFunction::IsNaNLogicalFunction(LogicalFunction child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE)), child(std::move(child))
{
}

bool IsNaNLogicalFunction::operator==(const IsNaNLogicalFunction& rhs) const
{
    return rhs.getChildren().size() == 1 and this->child == rhs.getChildren()[0];
}

std::string IsNaNLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("ISNAN({})", child.explain(verbosity));
}

LogicalFunction IsNaNLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    const auto inferredChild = child.withInferredDataType(schema);
    if (not inferredChild.getDataType().isNumeric())
    {
        throw CannotInferStamp("IS NaN expects a numeric input function but got {}", inferredChild.getDataType());
    }

    /// NaN is the only value that does not compare equal to itself, so IS NaN desugars into NOT(x = x) and needs no physical
    /// function of its own. Comparing against a NaN constant would not work, as every comparison with NaN is false, NaN = NaN
    /// included. Integers always compare equal to themselves and thus yield false, as required.
    /// Unlike ISNULL, IS NaN follows the usual nullability propagation, i.e., NULL IS NaN is NULL. Both Equals and Negate
    /// propagate NULL, so the desugared form keeps that behaviour.
    if (inferredChild.getDataType().isInteger())
    {
        const DataType trueDataType{
            DataType::Type::BOOLEAN,
            inferredChild.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE};
        return ConstantValueLogicalFunction{trueDataType, "false"};
    }
    return NegateLogicalFunction{EqualsLogicalFunction{inferredChild, inferredChild}}.withInferredDataType(schema);
}

DataType IsNaNLogicalFunction::getDataType() const
{
    return dataType;
};

IsNaNLogicalFunction IsNaNLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> IsNaNLogicalFunction::getChildren() const
{
    return {child};
};

IsNaNLogicalFunction IsNaNLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "IsNaNLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view IsNaNLogicalFunction::getType()
{
    return NAME;
}

Reflected Reflector<IsNaNLogicalFunction>::operator()(const IsNaNLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedIsNaNLogicalFunction{.child = std::make_optional<LogicalFunction>(function.child)});
}

IsNaNLogicalFunction Unreflector<IsNaNLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [function] = context.unreflect<detail::ReflectedIsNaNLogicalFunction>(reflected);
    if (!function.has_value())
    {
        throw CannotDeserialize("Failed to deserialize child of IsNaNLogicalFunction");
    }

    return IsNaNLogicalFunction(std::move(function.value()));
}

LogicalFunctionRegistryReturnType IsNaNLogicalFunction::createIsNaN(const LogicalFunctionRegistryArguments& arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("IsNaNLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return IsNaNLogicalFunction(arguments.children[0]);
}

}
