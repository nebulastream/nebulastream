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
#include <Functions/BinaryLogicalFunction.hpp>
#include <Functions/ConcatLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES
{

ConcatLogicalFunction::ConcatLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
    : BinaryLogicalFunction(left->getStamp()->join(right->getStamp()), left, right)
{
    PRECONDITION(
        NES::Util::instanceOf<VariableSizedDataType>(getLeftChild()->getStamp())
            and NES::Util::instanceOf<VariableSizedDataType>(getRightChild()->getStamp()),
        "Expected VariableSizedDataTypes");
}


std::shared_ptr<LogicalFunction> ConcatLogicalFunction::clone() const
{
    return std::make_shared<ConcatLogicalFunction>(
        Util::as<LogicalFunction>(getLeftChild())->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

bool ConcatLogicalFunction::operator==(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<ConcatLogicalFunction>(rhs))
    {
        const auto otherMulNode = NES::Util::as<ConcatLogicalFunction>(rhs);
        return getLeftChild() == otherMulNode->getLeftChild() and getRightChild() == otherMulNode->getRightChild();
    }
    return false;
}

std::string ConcatLogicalFunction::toString() const
{
    return fmt::format("Concat({}, {})", *getLeftChild(), *getRightChild());
}

SerializableFunction ConcatLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);

    auto* funcDesc = new SerializableFunction_BinaryFunction();
    auto* leftChild = funcDesc->mutable_leftchild();
    leftChild->CopyFrom(getLeftChild()->serialize());
    auto* rightChild = funcDesc->mutable_rightchild();
    rightChild->CopyFrom(getRightChild()->serialize());

    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

}
