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
#include <Functions/BooleanFunctions/IsNullCheckLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

IsNullCheckLogicalFunction::IsNullCheckLogicalFunction(LogicalFunction child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN)), child(std::move(child))
{
}

bool IsNullCheckLogicalFunction::operator==(const IsNullCheckLogicalFunction& rhs) const
{
    return rhs.getChildren().size() == 1 and this->child == rhs.getChildren()[0];
}

std::string IsNullCheckLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("NOT({})", child.explain(verbosity));
}

LogicalFunction IsNullCheckLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto newChild = child.withInferredDataType(schema);
    return withChildren({newChild});
}

DataType IsNullCheckLogicalFunction::getDataType() const
{
    return dataType;
};

IsNullCheckLogicalFunction IsNullCheckLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> IsNullCheckLogicalFunction::getChildren() const
{
    return {child};
};

IsNullCheckLogicalFunction IsNullCheckLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "IsNullCheckLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view IsNullCheckLogicalFunction::getType()
{
    return NAME;
}

Reflected Reflector<IsNullCheckLogicalFunction>::operator()(const IsNullCheckLogicalFunction& function) const
{
    return reflect(detail::ReflectedIsNullCheckLogicalFunction{.child = std::make_optional<LogicalFunction>(function.child)});
}

IsNullCheckLogicalFunction Unreflector<IsNullCheckLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [function] = unreflect<detail::ReflectedIsNullCheckLogicalFunction>(reflected);
    if (!function.has_value())
    {
        throw CannotDeserialize("Failed to deserialize child of NegateLogicalFunction");
    }

    return IsNullCheckLogicalFunction(std::move(function.value()));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterIsNullLogicalFunction(const LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<IsNullCheckLogicalFunction>(arguments.reflected);
    }

    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("IsNullCheckLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return IsNullCheckLogicalFunction(arguments.children[0]);
}

}
