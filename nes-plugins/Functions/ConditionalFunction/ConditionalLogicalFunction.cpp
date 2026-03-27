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
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

ConditionalLogicalFunction::ConditionalLogicalFunction(std::vector<LogicalFunction> children)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), children(std::move(children))
{
    if (!this->children.empty())
    {
        dataType = this->children.back().getDataType();
    }
}

bool ConditionalLogicalFunction::operator==(const ConditionalLogicalFunction& rhs) const
{
    return children == rhs.children;
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

LogicalFunction ConditionalLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> inferredChildren;
    inferredChildren.reserve(children.size());
    for (const auto& child : children)
    {
        inferredChildren.push_back(child.withInferredDataType(schema));
    }

    PRECONDITION(
        inferredChildren.size() >= 3 && inferredChildren.size() % 2 == 1,
        "ConditionalLogicalFunction requires an odd number of children >= 3, but got {}",
        inferredChildren.size());

    for (std::size_t i = 0; i + 1 < inferredChildren.size(); i += 2)
    {
        PRECONDITION(
            inferredChildren.at(i).getDataType().isType(DataType::Type::BOOLEAN),
            "ConditionalLogicalFunction: condition at index {} must be BOOLEAN, but was {}",
            i,
            inferredChildren.at(i).getDataType());
    }

    const auto resultType = inferredChildren.back().getDataType();
    for (std::size_t i = 1; i + 1 < inferredChildren.size(); i += 2)
    {
        PRECONDITION(
            inferredChildren.at(i).getDataType() == resultType,
            "ConditionalLogicalFunction: result at index {} has type {}, but expected {}",
            i,
            inferredChildren.at(i).getDataType(),
            resultType);
    }

    return withChildren(inferredChildren).withDataType(resultType);
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

std::string ConditionalLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string result = "CASE";
    for (std::size_t i = 0; i + 1 < children.size(); i += 2)
    {
        result += fmt::format(" WHEN {} THEN {}", children.at(i).explain(verbosity), children.at(i + 1).explain(verbosity));
    }
    result += fmt::format(" ELSE {} END", children.back().explain(verbosity));
    return result;
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

ConditionalLogicalFunction Unreflector<ConditionalLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [childrenOpts] = unreflect<detail::ReflectedConditionalLogicalFunction>(reflected);
    std::vector<LogicalFunction> children;
    children.reserve(childrenOpts.size());
    for (const auto& childOpt : childrenOpts)
    {
        if (!childOpt.has_value())
        {
            throw CannotDeserialize("ConditionalLogicalFunction child is missing");
        }
        children.push_back(*childOpt);
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
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ConditionalLogicalFunction>(arguments.reflected);
    }
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
