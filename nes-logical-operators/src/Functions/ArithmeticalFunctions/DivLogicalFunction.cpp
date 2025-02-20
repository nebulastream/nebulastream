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

namespace NES
{

DivLogicalFunction::DivLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right) : BinaryLogicalFunction(left->getStamp(), left, right)
{
};

DivLogicalFunction::DivLogicalFunction(const DivLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

bool DivLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<DivLogicalFunction>(rhs))
    {
        auto otherDivNode = NES::Util::as<DivLogicalFunction>(rhs);
        return getLeftChild() == otherDivNode->getLeftChild() and getRightChild() == otherDivNode->getRightChild();
    }
    return false;
}

std::string DivLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "/" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> DivLogicalFunction::clone() const
{
    return std::make_shared<DivLogicalFunction>(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

SerializableFunction DivLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_BinaryFunction();
    auto* leftChild = funcDesc->mutable_leftchild();
    leftChild->CopyFrom(getLeftChild()->serialize());
    auto* rightChild = funcDesc->mutable_rightchild();
    rightChild->CopyFrom(getRightChild()->serialize());

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

}
