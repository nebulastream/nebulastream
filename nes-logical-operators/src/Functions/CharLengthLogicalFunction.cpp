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

#include <Functions/CharLengthLogicalFunction.hpp>

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
CharLengthLogicalFunction::CharLengthLogicalFunction(const LogicalFunction& child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)), child(child)
{
}

bool CharLengthLogicalFunction::operator==(const CharLengthLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string CharLengthLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("CHAR_LENGTH({})", child.explain(verbosity));
}

DataType CharLengthLogicalFunction::getDataType() const
{
    return dataType;
};

CharLengthLogicalFunction CharLengthLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction CharLengthLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& chr : getChildren())
    {
        newChildren.push_back(chr.withInferredDataType(schema));
    }
    INVARIANT(newChildren.size() == 1, "CharLengthLogicalFunction expects exactly one child but has {}", newChildren.size());
    if (not newChildren[0].getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("CHAR_LENGTH expects a VARSIZED input but got {}", newChildren[0].getDataType());
    }
    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::UINT64);
    newDataType.nullable = newChildren[0].getDataType().nullable;
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> CharLengthLogicalFunction::getChildren() const
{
    return {child};
};

CharLengthLogicalFunction CharLengthLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::string_view CharLengthLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<CharLengthLogicalFunction>::operator()(const CharLengthLogicalFunction& function) const
{
    return reflect(detail::ReflectedCharLengthLogicalFunction{.child = function.child});
}

CharLengthLogicalFunction Unreflector<CharLengthLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedCharLengthLogicalFunction>(reflected);

    if (!child.has_value())
    {
        throw CannotDeserialize("CharLengthLogicalFunction is missing its child");
    }
    return CharLengthLogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterCHAR_LENGTHLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<CharLengthLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.empty())
    {
        throw CannotDeserialize("CHAR_LENGTH requires one argument");
    }
    return CharLengthLogicalFunction(arguments.children.back());
}

}
