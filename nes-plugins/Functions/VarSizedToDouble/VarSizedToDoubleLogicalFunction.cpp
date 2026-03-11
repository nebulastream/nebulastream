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

#include "VarSizedToDoubleLogicalFunction.hpp"

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
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
#include "DataTypes/DataType.hpp"

namespace NES
{

// VarSizedToDoubleLogicalFunction::VarSizedToDoubleLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child)
VarSizedToDoubleLogicalFunction::VarSizedToDoubleLogicalFunction(const LogicalFunction& child) : dataType(DataType::Type::FLOAT64), child(child)
{
}

DataType VarSizedToDoubleLogicalFunction::getDataType() const
{
    return dataType;
};

VarSizedToDoubleLogicalFunction VarSizedToDoubleLogicalFunction::withDataType(const DataType&) const
{
    auto copy = *this;
    // copy.dataType = dataType;
    return copy;
};

LogicalFunction VarSizedToDoubleLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    return withDataType(newChild.getDataType()).withChildren({newChild});
};

std::vector<LogicalFunction> VarSizedToDoubleLogicalFunction::getChildren() const
{
    return {child};
};

VarSizedToDoubleLogicalFunction VarSizedToDoubleLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "VarSizedToDoubleLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view VarSizedToDoubleLogicalFunction::getType() const
{
    return NAME;
}

bool VarSizedToDoubleLogicalFunction::operator==(const VarSizedToDoubleLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string VarSizedToDoubleLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("VarSizedToDoubleLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("VarSizedToDouble({})", child.explain(verbosity));
}

Reflected Reflector<VarSizedToDoubleLogicalFunction>::operator()(const VarSizedToDoubleLogicalFunction& function) const
{
    return reflect(detail::ReflectedVarSizedToDoubleLogicalFunction{.child = function.child});
}

VarSizedToDoubleLogicalFunction Unreflector<VarSizedToDoubleLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedVarSizedToDoubleLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("VarSizedToDoubleLogicalFunction is missing its child");
    }
    return VarSizedToDoubleLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterVarSizedToDoubleLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<VarSizedToDoubleLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("VarSizedToDoubleLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return VarSizedToDoubleLogicalFunction(arguments.children.front());
}

}
