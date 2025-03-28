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
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

OrLogicalFunction::OrLogicalFunction(OrLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

OrLogicalFunction::OrLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto orNode = std::make_shared<OrLogicalFunction>();
    orNode->setLeftChild(left);
    orNode->setRightChild(right);
    return orNode;
}

bool OrLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<OrLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<OrLogicalFunction>(rhs);
        const bool simpleMatch = getLeftChild() == other->getLeftChild() and getRightChild() == other->getRightChild();
        const bool commutativeMatch = getLeftChild() == other->getRightChild() and getRightChild() == other->getLeftChild();
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string OrLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "||" << *getRightChild();
    return ss.str();
}

void OrLogicalFunction::inferStamp(const Schema& schema)
{
    std::vector<LogicalFunction> children;
    /// delegate stamp inference of children
    LogicalFunction::inferStamp(schema);
    /// check if children stamp is correct
    INVARIANT(getLeftChild()->isPredicate(), "the stamp of left child must be boolean, but was: " + getLeftChild()->getStamp()->toString());
    INVARIANT(getRightChild()->isPredicate(), "the stamp of right child must be boolean, but was: " + getRightChild()->getStamp()->toString());
}

std::shared_ptr<LogicalFunction> OrLogicalFunction::clone() const
{
    return std::make_shared<OrLogicalFunction>(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

bool OrLogicalFunction::validateBeforeLowering() const
{
    return NES::Util::instanceOf<Boolean>(Util::as<LogicalFunction>(this->getLeftChild())->getStamp())
        && NES::Util::instanceOf<Boolean>(Util::as<LogicalFunction>(this->getRightChild())->getStamp());
}


}
