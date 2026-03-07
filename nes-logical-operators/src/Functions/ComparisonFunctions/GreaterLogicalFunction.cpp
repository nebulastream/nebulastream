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

#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>

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

GreaterLogicalFunction::GreaterLogicalFunction(LogicalFunction left, LogicalFunction right)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN)), left(std::move(left)), right(std::move(right))
{
}

bool GreaterLogicalFunction::operator==(const GreaterLogicalFunction& rhs) const
{
    const bool simpleMatch = left == rhs.left and right == rhs.right;
    const bool commutativeMatch = left == rhs.right and right == rhs.left;
    return simpleMatch or commutativeMatch;
}

std::string GreaterLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} > {}", left.explain(verbosity), right.explain(verbosity));
}

DataType GreaterLogicalFunction::getDataType() const
{
    return dataType;
};

GreaterLogicalFunction GreaterLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction GreaterLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return this->withChildren(newChildren);
}

std::vector<LogicalFunction> GreaterLogicalFunction::getChildren() const
{
    return {left, right};
};

GreaterLogicalFunction GreaterLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "GreaterLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view GreaterLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<GreaterLogicalFunction>::operator()(const GreaterLogicalFunction& function) const
{
    return reflect(detail::ReflectedGreaterLogicalFunction{.left = function.left, .right = function.right});
}

GreaterLogicalFunction Unreflector<GreaterLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [left, right] = context.unreflect<detail::ReflectedGreaterLogicalFunction>(reflected);
    return {left, right};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterGreaterLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return ReflectionContext{}.unreflect<GreaterLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("GreaterLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return GreaterLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
