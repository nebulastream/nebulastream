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

#include <Functions/OctetLengthLogicalFunction.hpp>

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
OctetLengthLogicalFunction::OctetLengthLogicalFunction(const LogicalFunction& child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)), child(child)
{
}

bool OctetLengthLogicalFunction::operator==(const OctetLengthLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string OctetLengthLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("OCTET_LENGTH({})", child.explain(verbosity));
}

DataType OctetLengthLogicalFunction::getDataType() const
{
    return dataType;
};

OctetLengthLogicalFunction OctetLengthLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction OctetLengthLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& chr : getChildren())
    {
        newChildren.push_back(chr.withInferredDataType(schema));
    }
    INVARIANT(newChildren.size() == 1, "OctetLengthLogicalFunction expects exactly one child but has {}", newChildren.size());
    if (not newChildren[0].getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("OCTET_LENGTH expects a VARSIZED input but got {}", newChildren[0].getDataType());
    }
    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::UINT64);
    newDataType.nullable = newChildren[0].getDataType().nullable;
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> OctetLengthLogicalFunction::getChildren() const
{
    return {child};
};

OctetLengthLogicalFunction OctetLengthLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::string_view OctetLengthLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<OctetLengthLogicalFunction>::operator()(const OctetLengthLogicalFunction& function) const
{
    return reflect(detail::ReflectedOctetLengthLogicalFunction{.child = function.child});
}

OctetLengthLogicalFunction Unreflector<OctetLengthLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedOctetLengthLogicalFunction>(reflected);

    if (!child.has_value())
    {
        throw CannotDeserialize("OctetLengthLogicalFunction is missing its child");
    }
    return OctetLengthLogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterOCTET_LENGTHLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<OctetLengthLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.empty())
    {
        throw CannotDeserialize("OCTET_LENGTH requires one argument");
    }
    return OctetLengthLogicalFunction(arguments.children.back());
}

}
