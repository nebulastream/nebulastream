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
#include <utility>
#include <Functions/ArithmeticalFunctions/NodeFunctionSub.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
namespace NES
{

NodeFunctionSub::NodeFunctionSub(DataTypePtr stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Sub") {};

NodeFunctionSub::NodeFunctionSub(NodeFunctionSub* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionSub::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto subNode = std::make_shared<NodeFunctionSub>(left->getStamp());
    subNode->setChildren(left, right);
    return subNode;
}

bool NodeFunctionSub::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionSub>(rhs))
    {
        auto otherSubNode = NES::Util::as<NodeFunctionSub>(rhs);
        return getLeft()->equal(otherSubNode->getLeft()) && getRight()->equal(otherSubNode->getRight());
    }
    return false;
}

std::string NodeFunctionSub::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "-" << children[1]->toString();
    return ss.str();
}

NodeFunctionPtr NodeFunctionSub::deepCopy()
{
bool NodeFunctionSub::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return this->getChildren()[0]->as<FunctionNode>()->getStamp()->isNumeric()
        && this->getChildren()[1]->as<FunctionNode>()->getStamp()->isNumeric();
}

}
