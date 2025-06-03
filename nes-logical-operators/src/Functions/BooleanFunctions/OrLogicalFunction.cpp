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

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
OrLogicalFunction::OrLogicalFunction(LogicalFunction left, LogicalFunction right)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , left(std::move(std::move(left)))
    , right(std::move(std::move(right)))
{
}

bool OrLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const OrLogicalFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string OrLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} OR {}", left.explain(verbosity), right.explain(verbosity));
}

DataType OrLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction OrLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> OrLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction OrLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "OrLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view OrLogicalFunction::getType() const
{
    return NAME;
}

LogicalFunction OrLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> children;
    /// delegate dataType inference of children
    for (auto& node : getChildren())
    {
        children.push_back(node.withInferredDataType(schema));
    }
    /// check if children dataType is correct
    INVARIANT(
        left.getDataType().isType(DataType::Type::BOOLEAN), "the dataType of left child must be boolean, but was: {}", left.getDataType());
    INVARIANT(
        right.getDataType().isType(DataType::Type::BOOLEAN),
        "the dataType of right child must be boolean, but was: {}",
        right.getDataType());
    return this->withChildren(children);
}

SerializableFunction OrLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterOrLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "OrLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return OrLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
