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

#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>

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
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

GreaterEqualsLogicalFunction::GreaterEqualsLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(std::move(left)), right(std::move(right)), dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
{
}

bool GreaterEqualsLogicalFunction::operator==(const GreaterEqualsLogicalFunction& rhs) const
{
    if (const auto* other = dynamic_cast<const GreaterEqualsLogicalFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string GreaterEqualsLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} >= {}", left.explain(verbosity), right.explain(verbosity));
}

DataType GreaterEqualsLogicalFunction::getDataType() const
{
    return dataType;
};

GreaterEqualsLogicalFunction GreaterEqualsLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction GreaterEqualsLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<LogicalFunction> GreaterEqualsLogicalFunction::getChildren() const
{
    return {left, right};
};

GreaterEqualsLogicalFunction GreaterEqualsLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "GreaterEqualsLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view GreaterEqualsLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<GreaterEqualsLogicalFunction>::operator()(const GreaterEqualsLogicalFunction& function) const
{
    return reflect(detail::ReflectedGreaterEqualsLogicalFunction{.left = function.left, .right = function.right});
}

GreaterEqualsLogicalFunction Unreflector<GreaterEqualsLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, right] = unreflect<detail::ReflectedGreaterEqualsLogicalFunction>(reflected);
    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("Missing child function");
    }
    return {left.value(), right.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterGreaterEqualsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<GreaterEqualsLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("GreaterEqualsLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return GreaterEqualsLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
