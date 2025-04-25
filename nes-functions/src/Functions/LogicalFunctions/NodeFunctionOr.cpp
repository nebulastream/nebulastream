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
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunctions/NodeFunctionOr.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
NodeFunctionOr::NodeFunctionOr() : NodeFunctionLogicalBinary("Or")
{
}

NodeFunctionOr::NodeFunctionOr(NodeFunctionOr* other) : NodeFunctionLogicalBinary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionOr::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto orNode = std::make_shared<NodeFunctionOr>();
    orNode->setChildren(left, right);
    return orNode;
}

bool NodeFunctionOr::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionOr>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionOr>(rhs);
        const bool simpleMatch = getLeft()->equal(other->getLeft()) and getRight()->equal(other->getRight());
        const bool commutativeMatch = getLeft()->equal(other->getRight()) and getRight()->equal(other->getLeft());
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::ostream& NodeFunctionOr::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << *children.at(0) << " || " << *children.at(1);
}

void NodeFunctionOr::inferStamp(const Schema& schema)
{
    /// delegate stamp inference of children
    NodeFunction::inferStamp(schema);
    /// check if children stamp is correct
    INVARIANT(getLeft()->isPredicate(), "the stamp of left child must be boolean, but was: {}", getLeft()->getStamp());
    INVARIANT(getRight()->isPredicate(), "the stamp of right child must be boolean, but was: {}", getRight()->getStamp());
}

std::shared_ptr<NodeFunction> NodeFunctionOr::deepCopy()
{
    return NodeFunctionOr::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

bool NodeFunctionOr::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return Util::as<NodeFunction>(this->getChildren()[0])->getStamp().isType(DataType::Type::BOOLEAN)
        and Util::as<NodeFunction>(this->getChildren()[1])->getStamp().isType(DataType::Type::BOOLEAN);
}


}
