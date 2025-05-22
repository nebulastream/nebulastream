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
#include <ostream>
#include <API/Schema.hpp>
#include <Functions/LogicalFunctions/NodeFunctionAnd.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>

namespace NES
{

NodeFunctionAnd::NodeFunctionAnd() : NodeFunctionLogicalBinary("And")
{
}

NodeFunctionAnd::NodeFunctionAnd(NodeFunctionAnd* other) : NodeFunctionLogicalBinary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionAnd::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto andNode = std::make_shared<NodeFunctionAnd>();
    andNode->setChildren(left, right);
    return andNode;
}

bool NodeFunctionAnd::equal(const std::shared_ptr<Node>& rhs) const
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

std::ostream& NodeFunctionAnd::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << *children.at(0) << " && " << *children.at(1);
}

void NodeFunctionAnd::inferStamp(const Schema& schema)
{
    /// delegate stamp inference of children
    NodeFunction::inferStamp(schema);
    /// check if children stamp is correct
    INVARIANT(getLeft()->isPredicate(), "the stamp of left child must be boolean, but was: {}", getLeft()->getStamp()->toString());
    INVARIANT(getRight()->isPredicate(), "the stamp of right child must be boolean, but was: {}", getRight()->getStamp()->toString());
}

std::shared_ptr<NodeFunction> NodeFunctionAnd::deepCopy()
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
