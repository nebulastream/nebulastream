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
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
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

AndLogicalFunction::AndLogicalFunction(const AndLogicalFunction& other) : dataType(other.dataType), left(other.left), right(other.right)
{
}

AndLogicalFunction::AndLogicalFunction(LogicalFunction left, LogicalFunction right)
    : dataType(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
    , left(std::move(std::move(left)))
    , right(std::move(std::move(right)))
{
}

std::shared_ptr<DataType> AndLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction AndLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> AndLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction AndLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "AndLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view AndLogicalFunction::getType() const
{
    return NAME;
}

bool AndLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const AndLogicalFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string AndLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} AND {}", left.explain(verbosity), right.explain(verbosity));
}

LogicalFunction AndLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& node : getChildren())
    {
        newChildren.push_back(node.withInferredDataType(schema));
    }
    /// check if children dataType is correct
    INVARIANT(
        *left.getDataType().get() == Boolean(),
        "the dataType of left child must be boolean, but was: {}",
        left.getDataType().get()->toString());
    INVARIANT(
        *left.getDataType().get() == Boolean(),
        "the dataType of right child must be boolean, but was: {}",
        right.getDataType().get()->toString());
    return this->withChildren(newChildren);
}

SerializableFunction AndLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(right.serialize());
    serializedFunction.add_children()->CopyFrom(left.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAndLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "AndLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return AndLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
