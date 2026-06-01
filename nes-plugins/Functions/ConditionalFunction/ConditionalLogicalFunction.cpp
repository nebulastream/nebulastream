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

#include "ConditionalLogicalFunction.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
ConditionalLogicalFunction::ConditionalLogicalFunction(std::vector<LogicalFunction> children)
    : dataType(children.back().getDataType()), children(std::move(children))
{
}

DataType ConditionalLogicalFunction::getDataType() const
{
    return dataType;
}

ConditionalLogicalFunction ConditionalLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

std::vector<LogicalFunction> ConditionalLogicalFunction::getChildren() const
{
    return children;
}

ConditionalLogicalFunction ConditionalLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    return ConditionalLogicalFunction(children);
}

std::string_view ConditionalLogicalFunction::getType() const
{
    return NAME;
}

bool ConditionalLogicalFunction::operator==(const ConditionalLogicalFunction& rhs) const
{
    return children == rhs.children;
}

std::string ConditionalLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string result = "CASE";
    for (size_t i = 0; i + 1 < children.size(); i += 2)
    {
        result += fmt::format(" WHEN {} THEN {}", children.at(i).explain(verbosity), children.at(i + 1).explain(verbosity));
    }
    result += fmt::format(" ELSE {}", children.back().explain(verbosity));
    return result;
}

LogicalFunction ConditionalLogicalFunction::withInferredDataType(const Schema&) const
{
    /// TODO step-2: infer the data type of this Conditional function.
    ///   * Recursively call withInferredDataType on every child.
    ///   * Validate: even-indexed children (0, 2, ...) are conditions, must be BOOLEAN.
    ///   * Validate: odd-indexed children (1, 3, ...) and the last child (default) must share the same type.
    ///   * Return withChildren(inferred).withDataType(resultType).
    throw NotImplemented("TODO step-2: implement ConditionalLogicalFunction::withInferredDataType");
}

Reflected Reflector<ConditionalLogicalFunction>::operator()(const ConditionalLogicalFunction& function) const
{
    detail::ReflectedConditionalLogicalFunction reflected;
    reflected.children.reserve(function.children.size());
    for (const auto& child : function.children)
    {
        reflected.children.push_back(std::make_optional(child));
    }
    return reflect(reflected);
}

ConditionalLogicalFunction
Unreflector<ConditionalLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [childrenOpts] = context.unreflect<detail::ReflectedConditionalLogicalFunction>(reflected);
    std::vector<LogicalFunction> children;
    children.reserve(childrenOpts.size());
    for (const auto& childOpt : childrenOpts)
    {
        if (!childOpt.has_value())
        {
            throw CannotDeserialize("ConditionalLogicalFunction child is missing");
        }
        children.push_back(childOpt.value());
    }
    if (children.size() < 3 || children.size() % 2 == 0)
    {
        throw CannotDeserialize("ConditionalLogicalFunction requires an odd number of children >= 3, but got {}", children.size());
    }
    return ConditionalLogicalFunction(std::move(children));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConditionalLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    /// Must have an odd number of children >= 3: at least one (condition, result) pair + default.
    if (arguments.children.size() < 3 || arguments.children.size() % 2 == 0)
    {
        throw CannotDeserialize(
            "ConditionalLogicalFunction requires an odd number of arguments >= 3 "
            "(condition/result pairs + default), but got {}",
            arguments.children.size());
    }
    return ConditionalLogicalFunction(std::move(arguments.children));
}
}
