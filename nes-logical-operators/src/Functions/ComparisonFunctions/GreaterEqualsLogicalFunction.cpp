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

#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <Schema/Schema.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

GreaterEqualsLogicalFunction::GreaterEqualsLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(std::move(std::move(left)))
    , right(std::move(std::move(right)))
{
}

bool GreaterEqualsLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GreaterEqualsLogicalFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string GreaterEqualsLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} >= {}", left.explain(verbosity), right.explain(verbosity));
}

DataType GreaterEqualsLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction GreaterEqualsLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    copy.left = copy.left.withInferredDataType(schema);
    copy.right = copy.right.withInferredDataType(schema);
    if (!copy.left.getDataType().isNumeric() || !copy.right.getDataType().isNumeric())
    {
        throw CannotInferStamp(
            "Can only apply greater equals to two functions with numeric data types, but got left: {}, right: {}",
            copy.left,
            copy.right);
    }
    copy.dataType = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
    return copy;

};

std::vector<LogicalFunction> GreaterEqualsLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction GreaterEqualsLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "GreaterEqualsLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view GreaterEqualsLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction GreaterEqualsLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterGreaterEqualsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("GreaterEqualsLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return GreaterEqualsLogicalFunction(arguments.children[0], arguments.children[1]).withInferredDataType(arguments.schema);
}

}
