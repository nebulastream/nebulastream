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
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

NodeFunctionEquals::NodeFunctionEquals() noexcept : NodeFunctionLogicalBinary("Equals")
{
}

NodeFunctionEquals::NodeFunctionEquals(NodeFunctionEquals* other) : NodeFunctionLogicalBinary(other)
{
}

NodeFunctionPtr NodeFunctionEquals::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto equals = std::make_shared<NodeFunctionEquals>();
    equals->setChildren(left, right);
    return equals;
}

bool NodeFunctionEquals::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionEquals>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionEquals>(rhs);
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::string NodeFunctionEquals::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "==" << children[1]->toString();
    return ss.str();
}

NodeFunctionPtr NodeFunctionEquals::deepCopy()
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
    if (childLeft->getStamp()->isText() != childRight->getStamp()->isText())
    {
        return false;
    }

    /// If one of the children has a stamp of type array, the other child must also have a stamp of type array
    if (childLeft->getStamp()->isArray() != childRight->getStamp()->isArray())
    {
        return false;
    }

    /// If one of the children has a stamp of type charArray, the other child must also have a stamp of type charArray
    if (childLeft->getStamp()->isCharArray() != childRight->getStamp()->isCharArray())
    {
        return false;
    }

    return true;
}

}
