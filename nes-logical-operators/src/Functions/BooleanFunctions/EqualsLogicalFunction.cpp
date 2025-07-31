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

#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
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

EqualsLogicalFunction::EqualsLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(std::move(std::move(left))), right(std::move(right))
{
}

bool EqualsLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const EqualsLogicalFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

DataType EqualsLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction EqualsLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    copy.left = copy.left.withInferredDataType(schema);
    copy.right = copy.right.withInferredDataType(schema);
    if (copy.left.getDataType() != copy.right.getDataType())
    {
        throw CannotInferStamp(
            "Can only apply equals to two functions with the same data type, but got left: {}, right: {}", copy.left, copy.right);
    }
    copy.dataType = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
    return copy;
};

std::vector<LogicalFunction> EqualsLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction EqualsLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "EqualsLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view EqualsLogicalFunction::getType() const
{
    return NAME;
}

std::string EqualsLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} = {}", left.explain(verbosity), right.explain(verbosity));
}

SerializableFunction EqualsLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());

    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());

    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterEqualsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("EqualsLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return EqualsLogicalFunction(arguments.children[0], arguments.children[1]).withInferredDataType(arguments.schema);
}

}
