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
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>

namespace NES
{

AddLogicalFunction::AddLogicalFunction(std::unique_ptr<LogicalFunction> left, std::unique_ptr<LogicalFunction> right)
    : BinaryLogicalFunction(std::move(left), std::move(right))
{
    stamp = left->getStamp().join(right->getStamp());
}

AddLogicalFunction::AddLogicalFunction(const AddLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

bool AddLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const AddLogicalFunction*>(&rhs);
    if (other)
    {
        const bool simpleMatch = getLeftChild() == other->getLeftChild() and getRightChild() == other->getRightChild();
        const bool commutativeMatch = getLeftChild() == other->getRightChild() and getRightChild() == other->getLeftChild();
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string AddLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << getLeftChild() << "+" << getRightChild();
    return ss.str();
}

std::unique_ptr<LogicalFunction> AddLogicalFunction::clone() const
{
    return std::make_unique<AddLogicalFunction>(getLeftChild().clone(), getRightChild().clone());
}

SerializableFunction AddLogicalFunction::serialize() const
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

BinaryLogicalFunctionRegistryReturnType
BinaryLogicalFunctionGeneratedRegistrar::RegisterAddBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<AddLogicalFunction>(std::move(arguments.leftChild), std::move(arguments.rightChild));
}

}
