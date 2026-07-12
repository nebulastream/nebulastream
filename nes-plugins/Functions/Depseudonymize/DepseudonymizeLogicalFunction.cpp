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

#include "DepseudonymizeLogicalFunction.hpp"

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

DepseudonymizeLogicalFunction::DepseudonymizeLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child)
{
}

bool DepseudonymizeLogicalFunction::operator==(const DepseudonymizeLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string DepseudonymizeLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("Depseudonymize({})", child.explain(verbosity));
}

DataType DepseudonymizeLogicalFunction::getDataType() const
{
    return dataType;
};

DepseudonymizeLogicalFunction DepseudonymizeLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction DepseudonymizeLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& chr : getChildren())
    {
        newChildren.push_back(chr.withInferredDataType(schema));
    }
    INVARIANT(newChildren.size() == 1, "DepseudonymizeLogicalFunction expects exactly one child but has {}", newChildren.size());

    const auto& inputType = newChildren[0].getDataType();

    // Accept either integer types (INT32, INT64, etc.) or variable-sized strings.
    // For integers: output type is the same integer type as input (preserves width).
    // For strings:  output type is always VARSIZED (hex string of the HMAC digest).
    if (inputType.isInteger())
    {
        // Integer path — keep same type as input
        return withDataType(inputType).withChildren(newChildren);
    }
    if (inputType.isType(DataType::Type::VARSIZED))
    {
        // String path — output is a VARSIZED hex string
        auto varsizedType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
        varsizedType.nullable = inputType.nullable;
        return withDataType(varsizedType).withChildren(newChildren);
    }

    throw DifferentFieldTypeExpected(
        "Depseudonymize expects an integer or VARSIZED input but got {}", inputType);
};

std::vector<LogicalFunction> DepseudonymizeLogicalFunction::getChildren() const
{
    return {child};
};

DepseudonymizeLogicalFunction DepseudonymizeLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "DepseudonymizeLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view DepseudonymizeLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<DepseudonymizeLogicalFunction>::operator()(const DepseudonymizeLogicalFunction& function) const
{
    return reflect(detail::ReflectedDepseudonymizeLogicalFunction{.child = function.child});
}

DepseudonymizeLogicalFunction Unreflector<DepseudonymizeLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedDepseudonymizeLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("DepseudonymizeLogicalFunction is missing its child");
    }
    return DepseudonymizeLogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterDepseudonymizeLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<DepseudonymizeLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Depseudonymize requires exactly one child, but got {}", arguments.children.size());
    }
    return DepseudonymizeLogicalFunction(arguments.children[0]);
}

}