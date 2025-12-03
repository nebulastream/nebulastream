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

#include <Functions/ArithmeticalFunctions/RoundLogicalFunction.hpp>

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

RoundLogicalFunction::RoundLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child) { };

bool RoundLogicalFunction::operator==(const RoundLogicalFunction& rhs) const
{
    if (const auto* other = dynamic_cast<const RoundLogicalFunction*>(&rhs))
    {
        return child == other->getChildren()[0];
    }
    return false;
}

std::string RoundLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("RoundLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("ROUND({})", child.explain(verbosity));
}

DataType RoundLogicalFunction::getDataType() const
{
    return dataType;
};

RoundLogicalFunction RoundLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction RoundLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    const auto childDataType = newChild.getDataType();
    return withDataType(childDataType).withChildren({newChild});
};

std::vector<LogicalFunction> RoundLogicalFunction::getChildren() const
{
    return {child};
};

RoundLogicalFunction RoundLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "RoundLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view RoundLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<RoundLogicalFunction>::operator()(const RoundLogicalFunction& function) const
{
    return reflect(detail::ReflectedRoundLogicalFunction{.child = function.child});
}

RoundLogicalFunction Unreflector<RoundLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedRoundLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("Missing child function");
    }
    return RoundLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterRoundLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<RoundLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Function requires exactly one child, but got {}", arguments.children.size());
    }
    return RoundLogicalFunction(arguments.children[0]);
}

}
