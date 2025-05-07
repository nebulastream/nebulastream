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
#include <API/Schema.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Util/Common.hpp>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>
#include <LogicalFunctions/LogicalFunctions/AndFunction.hpp>

namespace NES::Logical {

AndFunction::AndFunction(const AndFunction& other) : stamp(other.stamp), left(other.left), right(other.right)
{
}

AndFunction::AndFunction(Function left, Function right)
    : stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)), left(left), right(right)
{
}

std::shared_ptr<DataType> AndFunction::getStamp() const
{
    return stamp;
};

Function AndFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

std::vector<Function> AndFunction::getChildren() const
{
    return {left, right};
};

Function AndFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 2, "AndFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view AndFunction::getType() const
{
    return NAME;
}

bool AndFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const AndFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string AndFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} AND {}", left.explain(verbosity), right.explain(verbosity));
}

Function AndFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& node : getChildren())
    {
        newChildren.push_back(node.withInferredStamp(schema));
    }
    /// check if children stamp is correct
    INVARIANT(
        *left.getStamp().get() == Boolean(), "the stamp of left child must be boolean, but was: {}", left.getStamp().get()->toString());
    INVARIANT(
        *left.getStamp().get() == Boolean(), "the stamp of right child must be boolean, but was: {}", right.getStamp().get()->toString());
    return this->withChildren(newChildren);
}

bool AndFunction::validateBeforeLowering() const
{
    return dynamic_cast<const Boolean*>(left.getStamp().get()) && dynamic_cast<const Boolean*>(right.getStamp().get());
}

SerializableFunction AndFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(right.serialize());
    serializedFunction.add_children()->CopyFrom(left.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType FunctionGeneratedRegistrar::RegisterAndFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "AndFunction requires exactly two children, but got {}", arguments.children.size());
    return AndFunction(arguments.children[0], arguments.children[1]);
}

} // namespace NES::Logical
