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

#include "PseudonymizeLogicalFunction.hpp"

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
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

PseudonymizeLogicalFunction::PseudonymizeLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child)
{
}

bool PseudonymizeLogicalFunction::operator==(const PseudonymizeLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string PseudonymizeLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("Pseudonymize({})", child.explain(verbosity));
}

DataType PseudonymizeLogicalFunction::getDataType() const
{
    return dataType;
};

PseudonymizeLogicalFunction PseudonymizeLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction PseudonymizeLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& chr : getChildren())
    {
        newChildren.push_back(chr.withInferredDataType(schema));
    }
    INVARIANT(newChildren.size() == 1, "PseudonymizeLogicalFunction expects exactly one child but has {}", newChildren.size());
    if (not newChildren[0].getDataType().isInteger())
    {
        throw DifferentFieldTypeExpected("Pseudonymize expects an integer input but got {}", newChildren[0].getDataType());
    }
    auto newDataType = newChildren[0].getDataType();
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> PseudonymizeLogicalFunction::getChildren() const
{
    return {child};
};

PseudonymizeLogicalFunction PseudonymizeLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "PseudonymizeLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view PseudonymizeLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<PseudonymizeLogicalFunction>::operator()(const PseudonymizeLogicalFunction& function) const
{
    return reflect(detail::ReflectedPseudonymizeLogicalFunction{.child = function.child});
}

PseudonymizeLogicalFunction Unreflector<PseudonymizeLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedPseudonymizeLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("PseudonymizeLogicalFunction is missing its child");
    }
    return PseudonymizeLogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterPseudonymizeLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<PseudonymizeLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Pseudonymize requires exactly one child, but got {}", arguments.children.size());
    }
    return PseudonymizeLogicalFunction(arguments.children[0]);
}

}
