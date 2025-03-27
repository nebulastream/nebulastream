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
#include <Functions/LogicalFunctions/OrLogicalFunction.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{
OrLogicalFunction::OrLogicalFunction(const OrLogicalFunction& other) : stamp(other.stamp->clone()), left(other.left), right(other.right)
{
}

OrLogicalFunction::OrLogicalFunction(LogicalFunction left, LogicalFunction right) : stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)), left(left), right(right)
{}

bool OrLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const OrLogicalFunction*>(&rhs);
    if (other) {
        const bool simpleMatch = left == other->left and  right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string OrLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << "||" << right;
    return ss.str();
}

const DataType& OrLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction OrLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return *this;
};

std::vector<LogicalFunction> OrLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction OrLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string OrLogicalFunction::getType() const
{
    return std::string(NAME);
}

LogicalFunction OrLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> children;
    /// delegate stamp inference of children
    for (auto& node : getChildren())
    {
        children.push_back(node.withInferredStamp(schema));
    }
    /// check if children stamp is correct
    INVARIANT(left.getStamp() == Boolean(), "the stamp of left child must be boolean, but was: " + left.getStamp().toString());
    INVARIANT(right.getStamp() == Boolean(), "the stamp of right child must be boolean, but was: " + right.getStamp().toString());
    return this->withChildren(children);
}

bool OrLogicalFunction::validateBeforeLowering() const
{
    return dynamic_cast<const Boolean*>(&left.getStamp()) && dynamic_cast<const Boolean*>(&right.getStamp());
}

SerializableFunction OrLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_BinaryFunction();
    auto* leftChild = funcDesc->mutable_leftchild();
    leftChild->CopyFrom(left.serialize());
    auto* rightChild = funcDesc->mutable_rightchild();
    rightChild->CopyFrom(right.serialize());

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

BinaryLogicalFunctionRegistryReturnType
BinaryLogicalFunctionGeneratedRegistrar::RegisterOrBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return OrLogicalFunction(arguments.leftChild, arguments.rightChild);
}

}
