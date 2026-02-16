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

#include <Functions/CastFromUnixTimestampLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>

namespace NES
{

CastFromUnixTimestampLogicalFunction::CastFromUnixTimestampLogicalFunction(DataType outputType, LogicalFunction child)
    : outputType(std::move(outputType)), child(std::move(child))
{
}

SerializableFunction CastFromUnixTimestampLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    DataTypeSerializationUtil::serializeDataType(outputType, serializedFunction.mutable_data_type());
    serializedFunction.add_children()->CopyFrom(child.serialize());
    return serializedFunction;
}

bool CastFromUnixTimestampLogicalFunction::operator==(const CastFromUnixTimestampLogicalFunction& rhs) const
{
    return this->outputType == rhs.outputType && this->child == rhs.child;
}

DataType CastFromUnixTimestampLogicalFunction::getDataType() const
{
    return outputType;
}

CastFromUnixTimestampLogicalFunction CastFromUnixTimestampLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.outputType = dataType;
    return copy;
}

LogicalFunction CastFromUnixTimestampLogicalFunction::withInferredDataType(const Schema&) const
{
    auto copy = *this;

    // Output is ISO-8601 UTC string => VARSIZED
    copy.outputType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);

    return copy;
}

std::vector<LogicalFunction> CastFromUnixTimestampLogicalFunction::getChildren() const
{
    return {child};
}

CastFromUnixTimestampLogicalFunction CastFromUnixTimestampLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "CastFromUnixTimestampLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view CastFromUnixTimestampLogicalFunction::getType() const
{
    return NAME;
}

std::string CastFromUnixTimestampLogicalFunction::explain(ExplainVerbosity) const
{
    return fmt::format("Cast from unix timestamp (ms) to ISO-8601 UTC, outputType={}", outputType);
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterCastFromUnixTsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("CastFromUnixTimestampLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return CastFromUnixTimestampLogicalFunction(arguments.dataType, arguments.children[0]);
}

}
