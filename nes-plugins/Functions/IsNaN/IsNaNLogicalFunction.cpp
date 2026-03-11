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

#include "IsNaNLogicalFunction.hpp"

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
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
#include "DataTypes/DataType.hpp"

namespace NES
{

IsNaNLogicalFunction::IsNaNLogicalFunction(const LogicalFunction& child) : dataType(DataType::Type::BOOLEAN), child(child)
{
}

DataType IsNaNLogicalFunction::getDataType() const
{
    return dataType;
};

IsNaNLogicalFunction IsNaNLogicalFunction::withDataType(const DataType&) const
{
    auto copy = *this;
    // copy.dataType = dataType;
    // copy.dataType = dataType;
    return copy;
};

LogicalFunction IsNaNLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    return withDataType(newChild.getDataType()).withChildren({newChild});
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

std::string_view IsNaNLogicalFunction::getType() const
{
    return NAME;
}

bool IsNaNLogicalFunction::operator==(const IsNaNLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string IsNaNLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("IsNaNLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("ISNAN({})", child.explain(verbosity));
}

Reflected Reflector<IsNaNLogicalFunction>::operator()(const IsNaNLogicalFunction& function) const
{
    return reflect(detail::ReflectedIsNaNLogicalFunction{.child = function.child});
}

IsNaNLogicalFunction Unreflector<IsNaNLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedIsNaNLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("IsNaNLogicalFunction is missing its child");
    }
    return IsNaNLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterIsNaNLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<IsNaNLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("IsNaNLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return IsNaNLogicalFunction(arguments.children.front());
}

}
