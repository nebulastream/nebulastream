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

#include <Functions/TrimLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>
#include <Functions/LogicalFunction.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>

namespace NES
{

TrimLogicalFunction::TrimLogicalFunction(const LogicalFunction& child)
    : dataType(child.getDataType()), child(child)
{
}

bool TrimLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const TrimLogicalFunction*>(&rhs))
    {
        return child == other->child;
    }
    return false;
}

std::string TrimLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("TRIM({})", child.explain(verbosity));
}

DataType TrimLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction TrimLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction TrimLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto newChild = child.withInferredDataType(schema);
    if (not newChild.getDataType().isType(DataType::Type::VARSIZED))
    {
        throw CannotInferSchema("TRIM function requires a VARSIZED argument, but got: {}", newChild.getDataType());
    }
    return withChildren({newChild});
}

std::vector<LogicalFunction> TrimLogicalFunction::getChildren() const
{
    return {child};
}

LogicalFunction TrimLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    if (children.size() != 1)
    {
        throw InvalidQuerySyntax("TRIM requires exactly one argument, got {}", children.size());
    }
    auto copy = *this;
    copy.child = children[0];
    copy.dataType = children[0].getDataType();
    return copy;
}

std::string_view TrimLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction TrimLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTrimLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("TrimLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return TrimLogicalFunction(arguments.children[0]);
}

}