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

#include <Functions/FromBase64LogicalFunction.hpp>

#include <string>
#include <string_view>
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

FromBase64LogicalFunction::FromBase64LogicalFunction(const LogicalFunction& child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), child(child)
{
}

bool FromBase64LogicalFunction::operator==(const FromBase64LogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string FromBase64LogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("FROM_BASE64({})", child.explain(verbosity));
}

DataType FromBase64LogicalFunction::getDataType() const
{
    return dataType;
};

FromBase64LogicalFunction FromBase64LogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction FromBase64LogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& c : getChildren())
    {
        newChildren.push_back(c.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> FromBase64LogicalFunction::getChildren() const
{
    return {child};
};

FromBase64LogicalFunction FromBase64LogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view FromBase64LogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<FromBase64LogicalFunction>::operator()(const FromBase64LogicalFunction& function) const
{
    return reflect(detail::ReflectedFromBase64LogicalFunction{.child = function.child});
}

FromBase64LogicalFunction Unreflector<FromBase64LogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedFromBase64LogicalFunction>(reflected);

    if (!child.has_value())
    {
        throw CannotDeserialize("FromBase64LogicalFunction is missing its child");
    }
    return FromBase64LogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFROM_BASE64LogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<FromBase64LogicalFunction>(arguments.reflected);
    }
    if (arguments.children.empty())
    {
        throw CannotDeserialize("FROM_BASE64 requires one argument");
    }
    return FromBase64LogicalFunction(arguments.children.back());
}

}
