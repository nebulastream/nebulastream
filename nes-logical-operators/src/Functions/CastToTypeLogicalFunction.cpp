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

#include <Functions/CastToTypeLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

CastToTypeLogicalFunction::CastToTypeLogicalFunction(const DataType dataType, LogicalFunction child)
    : castToType(dataType), child(std::move(child))
{
}

SerializableFunction CastToTypeLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    DataTypeSerializationUtil::serializeDataType(castToType, serializedFunction.mutable_data_type());
    return serializedFunction;
}

bool CastToTypeLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const CastToTypeLogicalFunction*>(&rhs))
    {
        return *this == *other;
    }
    return false;
}

bool operator==(const CastToTypeLogicalFunction& lhs, const CastToTypeLogicalFunction& rhs)
{
    return rhs.castToType == lhs.castToType;
}

DataType CastToTypeLogicalFunction::getDataType() const
{
    return castToType;
}

LogicalFunction CastToTypeLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.castToType = dataType;
    return copy;
}

LogicalFunction CastToTypeLogicalFunction::withInferredDataType(const Schema&) const
{
    return *this;
}

std::vector<LogicalFunction> CastToTypeLogicalFunction::getChildren() const
{
    return {child};
}

LogicalFunction CastToTypeLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "CastToTypeLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view CastToTypeLogicalFunction::getType() const
{
    return NAME;
}

std::string CastToTypeLogicalFunction::explain(ExplainVerbosity) const
{
    return fmt::format("Cast to {})", castToType);
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterCastToTypeLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.children.size() == 1, "CastToTypeLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    return CastToTypeLogicalFunction(arguments.dataType, arguments.children[0]);
}

}
