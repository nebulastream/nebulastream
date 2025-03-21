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

#include <sstream>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>

namespace NES
{

DivLogicalFunction::DivLogicalFunction(LogicalFunction left, LogicalFunction right) : stamp(left.getStamp().clone()), left(left), right(right)
{
};

DivLogicalFunction::DivLogicalFunction(const DivLogicalFunction& other) : stamp(other.stamp->clone()), left(other.left), right(other.right)
{
}

bool DivLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const DivLogicalFunction*>(&rhs);
    if (other)
    {
        return left == other->left and right == other->right;
    }
    return false;
}

std::string DivLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << "/" << right;
    return ss.str();
}

SerializableFunction DivLogicalFunction::serialize() const
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
BinaryLogicalFunctionGeneratedRegistrar::RegisterDivBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return DivLogicalFunction(arguments.leftChild, arguments.rightChild);
}


}
