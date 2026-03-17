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

#include "UnixTimestampToDatetimeTestingLogicalFunction.hpp"

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

UnixTimestampToDatetimeTestingLogicalFunction::UnixTimestampToDatetimeTestingLogicalFunction(const LogicalFunction& child) : dataType(DataType::Type::VARSIZED), child(child)
{
}

DataType UnixTimestampToDatetimeTestingLogicalFunction::getDataType() const
{
    return dataType;
};

UnixTimestampToDatetimeTestingLogicalFunction UnixTimestampToDatetimeTestingLogicalFunction::withDataType(const DataType&) const
{
    auto copy = *this;
    // copy.dataType = dataType;
    // copy.dataType = dataType;
    return copy;
};

LogicalFunction UnixTimestampToDatetimeTestingLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    return withDataType(newChild.getDataType()).withChildren({newChild});
};

std::vector<LogicalFunction> UnixTimestampToDatetimeTestingLogicalFunction::getChildren() const
{
    return {child};
};

UnixTimestampToDatetimeTestingLogicalFunction UnixTimestampToDatetimeTestingLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "UnixTimestampToDatetimeTestingLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view UnixTimestampToDatetimeTestingLogicalFunction::getType() const
{
    return NAME;
}

bool UnixTimestampToDatetimeTestingLogicalFunction::operator==(const UnixTimestampToDatetimeTestingLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string UnixTimestampToDatetimeTestingLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("UnixTimestampToDatetimeTestingLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("UnixTimestampToDatetimeTesting({})", child.explain(verbosity));
}

Reflected Reflector<UnixTimestampToDatetimeTestingLogicalFunction>::operator()(const UnixTimestampToDatetimeTestingLogicalFunction& function) const
{
    return reflect(detail::ReflectedUnixTimestampToDatetimeTestingLogicalFunction{.child = function.child});
}

UnixTimestampToDatetimeTestingLogicalFunction Unreflector<UnixTimestampToDatetimeTestingLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedUnixTimestampToDatetimeTestingLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("UnixTimestampToDatetimeTestingLogicalFunction is missing its child");
    }
    return UnixTimestampToDatetimeTestingLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterUnixTimestampToDatetimeTestingLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<UnixTimestampToDatetimeTestingLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("UnixTimestampToDatetimeTestingLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return UnixTimestampToDatetimeTestingLogicalFunction(arguments.children.front());
}

}
