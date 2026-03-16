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

#include "HighAnionGapLogicalFunction.hpp"

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

HighAnionGapLogicalFunction::HighAnionGapLogicalFunction(const LogicalFunction& child) : dataType(DataType::Type::VARSIZED), child(child)
// HighAnionGapLogicalFunction::HighAnionGapLogicalFunction(const LogicalFunction& child) : dataType(DataType::Type::BOOLEAN), child(child)
{
}

DataType HighAnionGapLogicalFunction::getDataType() const
{
    return dataType;
};

HighAnionGapLogicalFunction HighAnionGapLogicalFunction::withDataType(const DataType&) const
{
    auto copy = *this;
    // copy.dataType = dataType;
    // copy.dataType = dataType;
    return copy;
};

LogicalFunction HighAnionGapLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    return withDataType(newChild.getDataType()).withChildren({newChild});
};

std::vector<LogicalFunction> HighAnionGapLogicalFunction::getChildren() const
{
    return {child};
};

HighAnionGapLogicalFunction HighAnionGapLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "HighAnionGapLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view HighAnionGapLogicalFunction::getType() const
{
    return NAME;
}

bool HighAnionGapLogicalFunction::operator==(const HighAnionGapLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string HighAnionGapLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("HighAnionGapLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("HighAnionGap({})", child.explain(verbosity));
}

Reflected Reflector<HighAnionGapLogicalFunction>::operator()(const HighAnionGapLogicalFunction& function) const
{
    return reflect(detail::ReflectedHighAnionGapLogicalFunction{.child = function.child});
}

HighAnionGapLogicalFunction Unreflector<HighAnionGapLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedHighAnionGapLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("HighAnionGapLogicalFunction is missing its child");
    }
    return HighAnionGapLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterHighAnionGapLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<HighAnionGapLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("HighAnionGapLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return HighAnionGapLogicalFunction(arguments.children.front());
}

}
