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
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

NodeFunctionEquals::NodeFunctionEquals() noexcept : NodeFunctionLogicalBinary("Equals")
{
}

NodeFunctionEquals::NodeFunctionEquals(NodeFunctionEquals* other) : NodeFunctionLogicalBinary(other)
{
}

std::shared_ptr<NodeFunction>
NodeFunctionEquals::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto equals = std::make_shared<NodeFunctionEquals>();
    equals->setChildren(left, right);
    return equals;
}

bool NodeFunctionEquals::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionEquals>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionEquals>(rhs);
        const bool simpleMatch = getLeft()->equal(other->getLeft()) and getRight()->equal(other->getRight());
        const bool commutativeMatch = getLeft()->equal(other->getRight()) and getRight()->equal(other->getLeft());
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::ostream& NodeFunctionEquals::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << *children.at(0) << " == " << *children.at(1);
}

std::shared_ptr<NodeFunction> NodeFunctionEquals::deepCopy()
{
    return NodeFunctionEquals::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

bool NodeFunctionEquals::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }

    const auto childLeft = Util::as<NodeFunction>(children[0]);
    const auto childRight = Util::as<NodeFunction>(children[1]);

    /// If one of the children has a stamp of type text, the other child must also have a stamp of type text
    if (childLeft->getStamp().physicalType.type == PhysicalType::Type::VARSIZED
        xor childRight->getStamp().physicalType.type == PhysicalType::Type::VARSIZED)
    {
        return false;
    }

    return true;
}

}
