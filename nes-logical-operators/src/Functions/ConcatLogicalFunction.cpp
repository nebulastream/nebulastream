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
#include <utility>
#include <Functions/LogicalFunction.hpp>
#include <Functions/BinaryLogicalFunction.hpp>
#include <Functions/ConcatLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <ErrorHandling.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>

namespace NES
{

ConcatLogicalFunction::ConcatLogicalFunction(std::unique_ptr<LogicalFunction> left, std::unique_ptr<LogicalFunction> right)
    : BinaryLogicalFunction(std::move(left),  std::move(right))
{
    stamp = left->getStamp().join(right->getStamp());
}


std::unique_ptr<LogicalFunction> ConcatLogicalFunction::clone() const
{
    return std::make_unique<ConcatLogicalFunction>(getLeftChild().clone(), getRightChild().clone());
}

bool ConcatLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const ConcatLogicalFunction*>(&rhs);
    if (other)
    {
        return getLeftChild() == other->getLeftChild() and getRightChild() == other->getRightChild();
    }
    return false;
}

std::string ConcatLogicalFunction::toString() const
{
    return fmt::format("Concat({}, {})", getLeftChild(), getRightChild());
}

SerializableFunction ConcatLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);

    auto* funcDesc = new SerializableFunction_BinaryFunction();
    auto* leftChild = funcDesc->mutable_leftchild();
    leftChild->CopyFrom(getLeftChild().serialize());
    auto* rightChild = funcDesc->mutable_rightchild();
    rightChild->CopyFrom(getRightChild().serialize());

    DataTypeSerializationUtil::serializeDataType(getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

BinaryLogicalFunctionRegistryReturnType
BinaryLogicalFunctionGeneratedRegistrar::RegisterConcatBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<ConcatLogicalFunction>(std::move(arguments.leftChild), std::move(arguments.rightChild));
}

}
