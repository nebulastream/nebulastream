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

#include <Functions/ArithmeticalFunctions/FloorLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ArithmeticalFunctions/FloorLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include "DataTypes/DataTypeProvider.hpp"

namespace NES
{

FloorLogicalFunction::FloorLogicalFunction(const LogicalFunction& child) : child(child) { };

DataType FloorLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction FloorLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    copy.child = child.withInferredDataType(schema);
    if (!copy.child.getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot apply floor function on non-numeric input function {}", copy.child);
    }
    if (copy.child.getDataType().isFloat())
    {
        copy.dataType = DataTypeProvider::provideDataType(DataType::Type::INT64);
    }
    copy.dataType = copy.child.getDataType();
    return copy;
};

std::vector<LogicalFunction> FloorLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction FloorLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "FloorLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view FloorLogicalFunction::getType() const
{
    return NAME;
}

bool FloorLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const FloorLogicalFunction*>(&rhs))
    {
        return child == other->child;
    }
    return false;
}

std::string FloorLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FloorLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("FLOOR({})", child.explain(verbosity));
}

SerializableFunction FloorLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFloorLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Function requires exactly one child, but got {}", arguments.children.size());
    }
    return FloorLogicalFunction(arguments.children[0]).withInferredDataType(arguments.schema);
}

}
