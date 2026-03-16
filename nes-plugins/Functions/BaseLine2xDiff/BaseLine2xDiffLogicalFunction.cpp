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

#include "BaseLine2xDiffLogicalFunction.hpp"

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

BaseLine2xDiffLogicalFunction::BaseLine2xDiffLogicalFunction(const LogicalFunction& child) : dataType(DataType::Type::VARSIZED), child(child)
// BaseLine2xDiffLogicalFunction::BaseLine2xDiffLogicalFunction(const LogicalFunction& child) : dataType(DataType::Type::BOOLEAN), child(child)
{
}

DataType BaseLine2xDiffLogicalFunction::getDataType() const
{
    return dataType;
};

BaseLine2xDiffLogicalFunction BaseLine2xDiffLogicalFunction::withDataType(const DataType&) const
{
    auto copy = *this;
    // copy.dataType = dataType;
    // copy.dataType = dataType;
    return copy;
};

LogicalFunction BaseLine2xDiffLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    return withDataType(newChild.getDataType()).withChildren({newChild});
};

std::vector<LogicalFunction> BaseLine2xDiffLogicalFunction::getChildren() const
{
    return {child};
};

BaseLine2xDiffLogicalFunction BaseLine2xDiffLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "BaseLine2xDiffLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view BaseLine2xDiffLogicalFunction::getType() const
{
    return NAME;
}

bool BaseLine2xDiffLogicalFunction::operator==(const BaseLine2xDiffLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string BaseLine2xDiffLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("BaseLine2xDiffLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("BaseLine2xDiff({})", child.explain(verbosity));
}

Reflected Reflector<BaseLine2xDiffLogicalFunction>::operator()(const BaseLine2xDiffLogicalFunction& function) const
{
    return reflect(detail::ReflectedBaseLine2xDiffLogicalFunction{.child = function.child});
}

BaseLine2xDiffLogicalFunction Unreflector<BaseLine2xDiffLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedBaseLine2xDiffLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("BaseLine2xDiffLogicalFunction is missing its child");
    }
    return BaseLine2xDiffLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterBaseLine2xDiffLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<BaseLine2xDiffLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("BaseLine2xDiffLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return BaseLine2xDiffLogicalFunction(arguments.children.front());
}

}
