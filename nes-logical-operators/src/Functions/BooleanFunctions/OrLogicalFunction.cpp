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

#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
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

namespace NES
{
OrLogicalFunction::OrLogicalFunction(LogicalFunction left, LogicalFunction right)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN)), left(std::move(left)), right(std::move(right))
{
}

bool OrLogicalFunction::operator==(const OrLogicalFunction& rhs) const
{
    const bool simpleMatch = left == rhs.left and right == rhs.right;
    const bool commutativeMatch = left == rhs.right and right == rhs.left;
    return simpleMatch or commutativeMatch;
}

std::string OrLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} OR {}", left.explain(verbosity), right.explain(verbosity));
}

DataType OrLogicalFunction::getDataType() const
{
    return dataType;
};

OrLogicalFunction OrLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> OrLogicalFunction::getChildren() const
{
    return {left, right};
};

OrLogicalFunction OrLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "OrLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view OrLogicalFunction::getType() const
{
    return NAME;
}

LogicalFunction OrLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> children;
    /// delegate dataType inference of children
    for (auto& node : getChildren())
    {
        children.push_back(node.withInferredDataType(schema));
    }
    /// check if children dataType is correct
    INVARIANT(
        children.at(0).getDataType().isType(DataType::Type::BOOLEAN),
        "the dataType of left child must be boolean, but was: {}",
        children.at(0).getDataType());
    INVARIANT(
        children.at(1).getDataType().isType(DataType::Type::BOOLEAN),
        "the dataType of right child must be boolean, but was: {}",
        children.at(1).getDataType());
    return this->withChildren(children);
}

Reflected Reflector<OrLogicalFunction>::operator()(const OrLogicalFunction& function) const
{
    return reflect(detail::ReflectedOrLogicalFunction{.left = function.left, .right = function.right});
}

OrLogicalFunction Unreflector<OrLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [left, right] = context.unreflect<detail::ReflectedOrLogicalFunction>(reflected);
    return {left, right};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterOrLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("OrLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return OrLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
