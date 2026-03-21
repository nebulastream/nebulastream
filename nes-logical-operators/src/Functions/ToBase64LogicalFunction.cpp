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

#include <Functions/ToBase64LogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp> /// NOLINT(misc-include-cleaner)
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h> /// NOLINT(misc-include-cleaner)

namespace NES
{

/// NOLINTNEXTLINE(modernize-pass-by-value)
ToBase64LogicalFunction::ToBase64LogicalFunction(const LogicalFunction& child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), child(child)
{
}

bool ToBase64LogicalFunction::operator==(const ToBase64LogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string ToBase64LogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("TO_BASE64({})", child.explain(verbosity));
}

DataType ToBase64LogicalFunction::getDataType() const
{
    return dataType;
};

ToBase64LogicalFunction ToBase64LogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalFunction ToBase64LogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& chr : getChildren())
    {
        newChildren.push_back(chr.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> ToBase64LogicalFunction::getChildren() const
{
    return {child};
};

ToBase64LogicalFunction ToBase64LogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::string_view ToBase64LogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<ToBase64LogicalFunction>::operator()(const ToBase64LogicalFunction& function) const
{
    return reflect(detail::ReflectedToBase64LogicalFunction{.child = function.child});
}

ToBase64LogicalFunction Unreflector<ToBase64LogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedToBase64LogicalFunction>(reflected);

    if (!child.has_value())
    {
        throw CannotDeserialize("ToBase64LogicalFunction is missing its child");
    }
    return ToBase64LogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterTO_BASE64LogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ToBase64LogicalFunction>(arguments.reflected);
    }
    if (arguments.children.empty())
    {
        throw CannotDeserialize("TO_BASE64 requires one argument");
    }
    return ToBase64LogicalFunction(arguments.children.back());
}

}
