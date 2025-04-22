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
#include <Functions/LogicalFunctions/OrBinaryLogicalFunction.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

OrBinaryLogicalFunction::OrBinaryLogicalFunction() : BinaryLogicalFunction("Or")
{
}

OrBinaryLogicalFunction::OrBinaryLogicalFunction(OrBinaryLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
OrBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto orNode = std::make_shared<OrBinaryLogicalFunction>();
    orNode->setChildren(left, right);
    return orNode;
}

bool OrBinaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<OrBinaryLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<OrBinaryLogicalFunction>(rhs);
        const bool simpleMatch = getLeft()->equal(other->getLeft()) and getRight()->equal(other->getRight());
        const bool commutativeMatch = getLeft()->equal(other->getRight()) and getRight()->equal(other->getLeft());
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string OrBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *children[0] << "||" << *children[1];
    return ss.str();
}

void OrBinaryLogicalFunction::inferStamp(const Schema& schema)
{
    /// delegate stamp inference of children
    LogicalFunction::inferStamp(schema);
    /// check if children stamp is correct
    INVARIANT(getLeft()->isPredicate(), "the stamp of left child must be boolean, but was: {}", getLeft()->getStamp()->toString());
    INVARIANT(getRight()->isPredicate(), "the stamp of right child must be boolean, but was: {}", getRight()->getStamp()->toString());
}

std::shared_ptr<LogicalFunction> OrBinaryLogicalFunction::deepCopy()
{
    return OrBinaryLogicalFunction::create(
        Util::as<LogicalFunction>(children[0])->deepCopy(), Util::as<LogicalFunction>(children[1])->deepCopy());
}

bool OrBinaryLogicalFunction::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return NES::Util::instanceOf<Boolean>(Util::as<LogicalFunction>(this->children[0])->getStamp())
        && NES::Util::instanceOf<Boolean>(Util::as<LogicalFunction>(this->children[1])->getStamp());
}


}
