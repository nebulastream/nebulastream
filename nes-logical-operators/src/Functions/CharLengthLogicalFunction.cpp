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
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/ScalarTypes.hpp>
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

CharLengthLogicalFunction::CharLengthLogicalFunction(LogicalFunction child) : child(std::move(child))
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

std::expected<NESType, Exception> CharLengthLogicalFunction::inferResultType(const NESType& input)
{
    if (unwrapOptionalNESType(input).tryGetAs<VarSizedType>() == nullptr)
    {
        return std::unexpected{DifferentFieldTypeExpected("CHAR_LENGTH expects a VARSIZED input")};
    }
    return wrapOptionalNESTypeIf(UInt64Type::makeNESType(), isOptionalNESType(input));
}

LogicalFunction CharLengthLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.child = child.withInferredDataType(schema);
    copy.dataType = requireLegacyDataType(inferResultType(copy.child.getDataType().toNESType()));
    return copy;
}

DataType CharLengthLogicalFunction::getDataType() const
{
    return dataType;
};

std::vector<LogicalFunction> CharLengthLogicalFunction::getChildren() const
{
    return {child};
};

CharLengthLogicalFunction CharLengthLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "CharLengthLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view CharLengthLogicalFunction::getType() const
{
    return NAME;
}

Reflected
Reflector<CharLengthLogicalFunction>::operator()(const CharLengthLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedCharLengthLogicalFunction{.child = function.child});
}

CharLengthLogicalFunction
Unreflector<CharLengthLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [child] = context.unreflect<detail::ReflectedCharLengthLogicalFunction>(reflected);
    return CharLengthLogicalFunction(std::move(child));
}

/// NOLINTNEXTLINE(readability-identifier-naming, readability-convert-member-functions-to-static)
LogicalFunctionRegistryReturnType CharLengthLogicalFunction::createCHAR_LENGTH(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("CHAR_LENGTH requires exactly one argument, but got {}", arguments.children.size());
    }
    return CharLengthLogicalFunction(arguments.children.back());
}

}
