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

#include "IsNaNLogicalFunction.hpp"

#include <string>
#include <string_view>
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
#include "DataTypes/DataType.hpp"

namespace NES
{

IsNaNLogicalFunction::IsNaNLogicalFunction(const LogicalFunction& child)
    : dataType(DataType::Type::BOOLEAN), child(child)
{
}

bool IsNaNLogicalFunction::operator==(const IsNaNLogicalFunction& rhs) const
{
    if (const auto* other = &rhs)
    {
        return child == other->child;
    }
    return false;
}

std::string IsNaNLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("IsNaN({})", child.explain(verbosity));
}

DataType IsNaNLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction IsNaNLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction IsNaNLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> IsNaNLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction IsNaNLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "IsNaNLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view IsNaNLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction IsNaNLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterIsNaNLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("IsNaNLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return IsNaNLogicalFunction(arguments.children[0]);
}

}
