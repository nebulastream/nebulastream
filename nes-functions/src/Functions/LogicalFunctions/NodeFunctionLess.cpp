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

#include <Functions/LogicalFunctions/NodeFunctionLess.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
namespace NES
{

NodeFunctionLess::NodeFunctionLess() : NodeFunctionLogicalBinary("Less")
{
}

NodeFunctionLess::NodeFunctionLess(NodeFunctionLess* other) : NodeFunctionLogicalBinary(other)
{
}

NodeFunctionPtr NodeFunctionLess::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto lessThen = std::make_shared<NodeFunctionLess>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool NodeFunctionLess::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<NodeFunctionLess>())
    {
        auto other = rhs->as<NodeFunctionLess>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::string NodeFunctionLess::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "<" << children[1]->toString();
    return ss.str();
}

NodeFunctionPtr NodeFunctionLess::deepCopy()
{
    return NodeFunctionLess::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool NodeFunctionLess::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }

    const auto childLeft = children[0]->as<FunctionNode>();
    const auto childRight = children[1]->as<FunctionNode>();

    /// If one of the children has a stamp of type text, we do not support comparison for text or arrays at the moment
    if (childLeft->getStamp()->isText() || childRight->getStamp()->isText() || childLeft->getStamp()->isArray()
        || childRight->getStamp()->isArray())
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
