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
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/BinaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>

namespace NES
{

ModuloLogicalFunction::ModuloLogicalFunction(const ModuloLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

ModuloLogicalFunction::ModuloLogicalFunction(std::unique_ptr<LogicalFunction> left, std::unique_ptr<LogicalFunction> right)
    : BinaryLogicalFunction(left->getStamp().join(right->getStamp()), std::move(left), std::move(right))
{
}

bool ModuloLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const ModuloLogicalFunction*>(&rhs);
    if(other)
    {
        return getLeftChild() == other->getLeftChild() and getRightChild() == other->getRightChild();
    }
    return false;
}

std::string ModuloLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << getLeftChild() << "%" << getRightChild();
    return ss.str();
}

std::unique_ptr<LogicalFunction> ModuloLogicalFunction::clone() const
{
    return std::make_unique<ModuloLogicalFunction>(getLeftChild().clone(), getRightChild().clone());
}

SerializableFunction ModuloLogicalFunction::serialize() const
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
BinaryLogicalFunctionGeneratedRegistrar::RegisterModuloBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<ModuloLogicalFunction>(std::move(arguments.leftChild), std::move(arguments.rightChild));
}


}
