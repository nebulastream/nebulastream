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
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

OrLogicalFunction::OrLogicalFunction(const OrLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

OrLogicalFunction::OrLogicalFunction(std::unique_ptr<LogicalFunction> left, std::unique_ptr<LogicalFunction> right)
    : BinaryLogicalFunction(DataTypeProvider::provideDataType(LogicalType::BOOLEAN), std::move(left), std::move(right))
{
}

bool OrLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const OrLogicalFunction*>(&rhs);
    if (other) {
        const bool simpleMatch = getLeftChild() == other->getLeftChild() and getRightChild() == other->getRightChild();
        const bool commutativeMatch = getLeftChild() == other->getRightChild() and getRightChild() == other->getLeftChild();
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string OrLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << getLeftChild() << "||" << getRightChild();
    return ss.str();
}

void OrLogicalFunction::inferStamp(const Schema& schema)
{
    std::vector<LogicalFunction> children;
    /// delegate stamp inference of children
    LogicalFunction::inferStamp(schema);
    /// check if children stamp is correct
    INVARIANT(getLeftChild().isPredicate(), "the stamp of left child must be boolean, but was: " + getLeftChild().getStamp().toString());
    INVARIANT(getRightChild().isPredicate(), "the stamp of right child must be boolean, but was: " + getRightChild().getStamp().toString());
}

std::unique_ptr<LogicalFunction> OrLogicalFunction::clone() const
{
    return std::make_unique<OrLogicalFunction>(getLeftChild().clone(), getRightChild().clone());
}

bool OrLogicalFunction::validateBeforeLowering() const
{
    return dynamic_cast<Boolean*>(&getLeftChild().getStamp())
        && dynamic_cast<Boolean*>(&getRightChild().getStamp());
}

SerializableFunction OrLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_BinaryFunction();
    auto* leftChild = funcDesc->mutable_leftchild();
    leftChild->CopyFrom(getLeftChild().serialize());
    auto* rightChild = funcDesc->mutable_rightchild();
    rightChild->CopyFrom(getRightChild().serialize());

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

std::unique_ptr<BinaryLogicalFunctionRegistryReturnType>
BinaryLogicalFunctionGeneratedRegistrar::RegisterOrBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<OrLogicalFunction>(std::move(arguments.leftChild), std::move(arguments.rightChild));
}

}
