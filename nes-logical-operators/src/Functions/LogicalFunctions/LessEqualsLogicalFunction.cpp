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

#include <Functions/LogicalFunctions/LessEqualsLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

LessEqualsLogicalFunction::LessEqualsLogicalFunction(const LessEqualsLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

LessEqualsLogicalFunction::LessEqualsLogicalFunction(std::unique_ptr<LogicalFunction> left, std::unique_ptr<LogicalFunction> right)
    : BinaryLogicalFunction(DataTypeProvider::provideDataType(LogicalType::BOOLEAN), std::move(left), std::move(right))
{
}

bool LessEqualsLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const LessEqualsLogicalFunction*>(&rhs);
    if (other)
    {
        return this->getLeftChild() == other->getLeftChild() && this->getRightChild() == other->getRightChild();
    }
    return false;
}

std::string LessEqualsLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << getLeftChild() << "<=" << getRightChild();
    return ss.str();
}

std::unique_ptr<LogicalFunction> LessEqualsLogicalFunction::clone() const
{
    return std::make_unique<LessEqualsLogicalFunction>(getLeftChild().clone(), getRightChild().clone());
}

SerializableFunction LessEqualsLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_BinaryFunction();
    auto* leftChild = funcDesc->mutable_leftchild();
    leftChild->CopyFrom(getLeftChild().serialize());
    auto* rightChild = funcDesc->mutable_rightchild();
    rightChild->CopyFrom(getRightChild().serialize());

    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

std::unique_ptr<BinaryLogicalFunctionRegistryReturnType>
BinaryLogicalFunctionGeneratedRegistrar::RegisterLessEqualsBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<LessEqualsLogicalFunction>(std::move(arguments.leftChild), std::move(arguments.rightChild));
}

}
