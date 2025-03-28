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

#include <Functions/LogicalFunctions/GreaterLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

GreaterLogicalFunction::GreaterLogicalFunction(const GreaterLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

GreaterLogicalFunction::GreaterLogicalFunction(std::unique_ptr<LogicalFunction> left, std::unique_ptr<LogicalFunction> right)
    : BinaryLogicalFunction(std::move(left), std::move(right))
{
    stamp = DataTypeProvider::provideDataType(LogicalType::BOOLEAN);
}

bool GreaterLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const GreaterLogicalFunction*>(&rhs);
    if (other)
    {
        return this->getLeftChild() == other->getLeftChild() && this->getRightChild() == other->getRightChild();
    }
    return false;
}

std::string GreaterLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << getLeftChild() << ">" << getRightChild();
    return ss.str();
}

std::unique_ptr<LogicalFunction> GreaterLogicalFunction::clone() const
{
    return std::make_unique<GreaterLogicalFunction>(getLeftChild().clone(), getRightChild().clone());
}

SerializableFunction GreaterLogicalFunction::serialize() const
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
BinaryLogicalFunctionGeneratedRegistrar::RegisterGreaterBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<GreaterLogicalFunction>(std::move(arguments.leftChild), std::move(arguments.rightChild));
}

}
