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

#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

NegateLogicalFunction::NegateLogicalFunction(LogicalFunction child) : child(std::move(child))
{
}

bool NegateLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const NegateLogicalFunction*>(&rhs))
    {
        return this->child == other->getChildren()[0];
    }
    return false;
}

std::string NegateLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("NOT({})", child.explain(verbosity));
}

LogicalFunction NegateLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    copy.child = child.withInferredDataType(schema);
    if (not copy.child.getDataType().isType(DataType::Type::BOOLEAN))
    {
        throw CannotInferSchema("Negate Function Node: the dataType of child must be boolean, but was: {}", copy.child.getDataType());
    }
    copy.dataType = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
    return copy;
}

DataType NegateLogicalFunction::getDataType() const
{
    return dataType;
};

std::vector<LogicalFunction> NegateLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction NegateLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "NegateLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view NegateLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction NegateLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterNegateLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("NegateLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    if (arguments.children[0].getDataType().type != DataType::Type::BOOLEAN)
    {
        throw CannotDeserialize("requires child of type bool, but got {}", arguments.children[0].getDataType());
    }
    return NegateLogicalFunction(arguments.children[0]).withInferredDataType(arguments.schema);
}

}
