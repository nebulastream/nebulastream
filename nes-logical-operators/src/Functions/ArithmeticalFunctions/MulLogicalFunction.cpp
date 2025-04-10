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
#include <sstream>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>

namespace NES
{
MulLogicalFunction::MulLogicalFunction(LogicalFunction left, LogicalFunction right) : stamp(left.getStamp()->join(right.getStamp())), left(left), right(right)
{}

MulLogicalFunction::MulLogicalFunction(const MulLogicalFunction& other) : stamp(other.stamp), left(other.left), right(other.right)
{
}

bool MulLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const MulLogicalFunction*>(&rhs);
    if (other)
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string MulLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << "*" << right;
    return ss.str();
}

std::shared_ptr<DataType> MulLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction MulLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction MulLogicalFunction::withInferredStamp(Schema schema) const
{
    auto copy = *this;
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    std::cout << "Mul: " << newChildren.at(0).getStamp()->toString() << "\n";
    std::cout << "Mul: " << newChildren.at(1).getStamp()->toString() << "\n";
    copy.stamp = newChildren[0].getStamp()->join(newChildren[1].getStamp());
    return copy.withChildren(newChildren);
};

std::vector<LogicalFunction> MulLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction MulLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.stamp = children[0].getStamp()->join(children[1].getStamp());
    return copy;
};

std::string MulLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction MulLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

BinaryLogicalFunctionRegistryReturnType
BinaryLogicalFunctionGeneratedRegistrar::RegisterMulBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return MulLogicalFunction(arguments.leftChild, arguments.rightChild);
}

}
