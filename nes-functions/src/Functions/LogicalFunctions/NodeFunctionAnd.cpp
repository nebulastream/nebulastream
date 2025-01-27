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

#include <API/Schema.hpp>
#include <Functions/LogicalFunctions/NodeFunctionAnd.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>
#include "Nodes/Node.hpp"

namespace NES
{

NodeFunctionAnd::NodeFunctionAnd() : NodeFunctionLogicalBinary("And")
{
}

NodeFunctionAnd::NodeFunctionAnd(NodeFunctionAnd* other) : NodeFunctionLogicalBinary(other)
{
}

NodeFunctionPtr NodeFunctionAnd::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto andNode = std::make_shared<NodeFunctionAnd>();
    andNode->setChildren(left, right);
    return andNode;
}

bool NodeFunctionAnd::equal(const NodePtr& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionAnd>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionAnd>(rhs);
        const bool simpleMatch = getLeft()->equal(other->getLeft()) and getRight()->equal(other->getRight());
        const bool commutativeMatch = getLeft()->equal(other->getRight()) and getRight()->equal(other->getLeft());
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string NodeFunctionAnd::toString() const
{
    std::stringstream ss;
    ss << *children[0] << "&&" << *children[1];
    return ss.str();
}

void NodeFunctionAnd::inferStamp(const Schema& schema)
{
    /// delegate stamp inference of children
    NodeFunction::inferStamp(schema);
    /// check if children stamp is correct
    INVARIANT(getLeft()->isPredicate(), "the stamp of left child must be boolean, but was: " + getLeft()->getStamp()->toString());
    INVARIANT(getRight()->isPredicate(), "the stamp of right child must be boolean, but was: " + getRight()->getStamp()->toString());
}
NodeFunctionPtr NodeFunctionAnd::deepCopy()
{
    return NodeFunctionAnd::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

bool NodeFunctionAnd::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return NES::Util::instanceOf<Boolean>(Util::as<NodeFunction>(this->getChildren()[0])->getStamp())
        && NES::Util::instanceOf<Boolean>(Util::as<NodeFunction>(this->getChildren()[1])->getStamp());
}


}
