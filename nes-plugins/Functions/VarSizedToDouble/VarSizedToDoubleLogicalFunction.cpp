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
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include "DataTypes/DataType.hpp"

namespace NES
{

VarSizedToDoubleLogicalFunction::VarSizedToDoubleLogicalFunction(const LogicalFunction& child)
    : dataType(DataType::Type::FLOAT64), child(child)
{
}

bool VarSizedToDoubleLogicalFunction::operator==(const VarSizedToDoubleLogicalFunction& rhs) const
{
    if (const auto* other = &rhs)
    {
        return child == other->child;
    }
    return false;
}

std::string VarSizedToDoubleLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("VARSIZEDTODOUBLE({})", child.explain(verbosity));
}

DataType VarSizedToDoubleLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction VarSizedToDoubleLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction VarSizedToDoubleLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> VarSizedToDoubleLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction VarSizedToDoubleLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
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

SerializableFunction VarSizedToDoubleLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterVarSizedToDoubleLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("VarSizedToDoubleLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return VarSizedToDoubleLogicalFunction(arguments.children[0]);
}

}
