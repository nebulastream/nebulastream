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

#include "UnixTimestampToDatetimeLogicalFunction.hpp"

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

UnixTimestampToDatetimeLogicalFunction::UnixTimestampToDatetimeLogicalFunction(const LogicalFunction& child) : dataType(DataType::Type::VARSIZED), child(child)
{
}

DataType UnixTimestampToDatetimeLogicalFunction::getDataType() const
{
    return dataType;
};

UnixTimestampToDatetimeLogicalFunction UnixTimestampToDatetimeLogicalFunction::withDataType(const DataType&) const
{
    auto copy = *this;
    // copy.dataType = dataType;
    // copy.dataType = dataType;
    return copy;
};

LogicalFunction UnixTimestampToDatetimeLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    return withDataType(newChild.getDataType()).withChildren({newChild});
};

std::vector<LogicalFunction> UnixTimestampToDatetimeLogicalFunction::getChildren() const
{
    return {child};
};

UnixTimestampToDatetimeLogicalFunction UnixTimestampToDatetimeLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "UnixTimestampToDatetimeLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view UnixTimestampToDatetimeLogicalFunction::getType() const
{
    return NAME;
}

bool UnixTimestampToDatetimeLogicalFunction::operator==(const UnixTimestampToDatetimeLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string UnixTimestampToDatetimeLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("UnixTimestampToDatetimeLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("UnixTimestampToDatetime({})", child.explain(verbosity));
}

Reflected Reflector<UnixTimestampToDatetimeLogicalFunction>::operator()(const UnixTimestampToDatetimeLogicalFunction& function) const
{
    return reflect(detail::ReflectedUnixTimestampToDatetimeLogicalFunction{.child = function.child});
}

UnixTimestampToDatetimeLogicalFunction Unreflector<UnixTimestampToDatetimeLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedUnixTimestampToDatetimeLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("UnixTimestampToDatetimeLogicalFunction is missing its child");
    }
    return UnixTimestampToDatetimeLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterUnixTimestampToDatetimeLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<UnixTimestampToDatetimeLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("UnixTimestampToDatetimeLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return UnixTimestampToDatetimeLogicalFunction(arguments.children.front());
}

}
