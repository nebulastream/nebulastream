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
#include <Functions/LogicalFunctions/LessLogicalFunction.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

LessLogicalFunction::LessLogicalFunction(const LessLogicalFunction& other) : left(other.left), right(other.right), stamp(other.stamp->clone())
{
}

LessLogicalFunction::LessLogicalFunction(LogicalFunction left, LogicalFunction right) : left(left), right(right), stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
{
}

bool LessLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const LessLogicalFunction*>(&rhs);
    if (other)
    {
        return this->left == other->left && this->right == other->right;
    }
    return false;
}

std::string LessLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << "<" << right;
    return ss.str();
}

SerializableFunction LessLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_BinaryFunction();
    auto* leftChild = funcDesc->mutable_leftchild();
    leftChild->CopyFrom(left.serialize());
    auto* rightChild = funcDesc->mutable_rightchild();
    rightChild->CopyFrom(right.serialize());

    DataTypeSerializationUtil::serializeDataType(getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

BinaryLogicalFunctionRegistryReturnType
BinaryLogicalFunctionGeneratedRegistrar::RegisterLessBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return LessLogicalFunction(arguments.leftChild, arguments.rightChild);
}

}
