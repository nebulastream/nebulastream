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

#include <Functions/CastToUnixTimestampLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

CastToUnixTimestampLogicalFunction::CastToUnixTimestampLogicalFunction(LogicalFunction child)
    : outputType(DataType{DataType::Type::UNDEFINED}), child(std::move(child))
{
}

bool CastToUnixTimestampLogicalFunction::operator==(const CastToUnixTimestampLogicalFunction& rhs) const
{
    return this->outputType == rhs.outputType && this->child == rhs.child;
}

DataType CastToUnixTimestampLogicalFunction::getDataType() const
{
    return outputType;
}

CastToUnixTimestampLogicalFunction CastToUnixTimestampLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.outputType = dataType;
    return copy;
}

LogicalFunction CastToUnixTimestampLogicalFunction::withInferredDataType(const Schema&) const
{
    auto copy = *this;
    copy.outputType = DataTypeProvider::provideDataType(DataType::Type::UINT64);
    return copy;
}

std::vector<LogicalFunction> CastToUnixTimestampLogicalFunction::getChildren() const
{
    return {child};
}

CastToUnixTimestampLogicalFunction CastToUnixTimestampLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "CastToUnixTimestampLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view CastToUnixTimestampLogicalFunction::getType() const
{
    return NAME;
}

std::string CastToUnixTimestampLogicalFunction::explain(ExplainVerbosity) const
{
    return fmt::format("Cast to unix timestamp (ms), outputType={}", outputType);
}

Reflected Reflector<CastToUnixTimestampLogicalFunction>::operator()(const CastToUnixTimestampLogicalFunction& function) const
{
    return reflect(detail::ReflectedCastToUnixTimestampLogicalFunction{.child = function.child});
}

CastToUnixTimestampLogicalFunction Unreflector<CastToUnixTimestampLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [function] = unreflect<detail::ReflectedCastToUnixTimestampLogicalFunction>(reflected);

    if (!function.has_value())
    {
        throw CannotDeserialize("Failed to deserialize child of CastToUnixTimestampLogicalFunction");
    }
    return CastToUnixTimestampLogicalFunction(std::move(function.value()));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterCastToUnixTsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<CastToUnixTimestampLogicalFunction>(arguments.reflected);
    }

    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("CastToUnixTimestampLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return CastToUnixTimestampLogicalFunction(arguments.children[0]);
}

}
