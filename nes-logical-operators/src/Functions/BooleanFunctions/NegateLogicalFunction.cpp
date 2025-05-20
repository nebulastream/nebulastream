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

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

NegateLogicalFunction::NegateLogicalFunction(LogicalFunction child)
    : dataType(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)), child(std::move(std::move(child)))
{
}

NegateLogicalFunction::NegateLogicalFunction(const NegateLogicalFunction& other) : dataType(other.dataType), child(other.child)
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
    auto newChild = child.withInferredDataType(schema);
    if (*newChild.getDataType() != Boolean())
    {
        throw CannotInferSchema(
            "Negate Function Node: the dataType of child must be boolean, but was: {}", child.getDataType()->toString());
    }
    return withChildren({newChild});
}

std::shared_ptr<DataType> NegateLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction NegateLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
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
    PRECONDITION(arguments.children.size() == 1, "NegateLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    return NegateLogicalFunction(arguments.children[0]);
}

}
