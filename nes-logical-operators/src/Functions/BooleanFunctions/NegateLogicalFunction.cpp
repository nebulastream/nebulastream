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

#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>

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

NegateLogicalFunction::NegateLogicalFunction(LogicalFunction child)
    : child(std::move(child)), dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
{
}

bool NegateLogicalFunction::operator==(const NegateLogicalFunction& rhs) const
{
    return this->child == rhs.getChildren()[0];
}

std::string NegateLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("NOT({})", child.explain(verbosity));
}

LogicalFunction NegateLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto newChild = child.withInferredDataType(schema);
    if (not newChild.getDataType().isType(DataType::Type::BOOLEAN))
    {
        throw CannotInferSchema("Negate Function Node: the dataType of child must be boolean, but was: {}", child.getDataType());
    }
    return withChildren({newChild});
}

DataType NegateLogicalFunction::getDataType() const
{
    return dataType;
};

NegateLogicalFunction NegateLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> NegateLogicalFunction::getChildren() const
{
    return {child};
};

NegateLogicalFunction NegateLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "NegateLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view NegateLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<NegateLogicalFunction>::operator()(const NegateLogicalFunction& function) const
{
    return reflect(detail::ReflectedNegateLogicalFunction{.child = function.child});
}

NegateLogicalFunction Unreflector<NegateLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [function] = unreflect<detail::ReflectedNegateLogicalFunction>(reflected);

    if (!function.has_value())
    {
        throw CannotDeserialize("Failed to deserialize child of NegateLogicalFunction");
    }
    if (function->getDataType().type != DataType::Type::BOOLEAN)
    {
        throw CannotDeserialize("requires child of type bool, but got {}", function->getDataType());
    }

    return NegateLogicalFunction(std::move(function.value()));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterNegateLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<NegateLogicalFunction>(arguments.reflected);
    }

    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("NegateLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    if (arguments.children[0].getDataType().type != DataType::Type::BOOLEAN)
    {
        throw CannotDeserialize("requires child of type bool, but got {}", arguments.children[0].getDataType());
    }
    return NegateLogicalFunction(arguments.children[0]);
}

}
