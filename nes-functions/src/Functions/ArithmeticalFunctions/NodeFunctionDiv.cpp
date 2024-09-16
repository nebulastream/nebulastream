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
#include <Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES
{

NodeFunctionDiv::NodeFunctionDiv(DataTypePtr stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Div") {};

NodeFunctionDiv::NodeFunctionDiv(NodeFunctionDiv* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionDiv::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto divNode = std::make_shared<NodeFunctionDiv>(left->getStamp());
    divNode->setChildren(left, right);
    return divNode;
}

bool NodeFunctionDiv::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<NodeFunctionDiv>())
    {
        auto otherDivNode = rhs->as<NodeFunctionDiv>();
        return getLeft()->equal(otherDivNode->getLeft()) && getRight()->equal(otherDivNode->getRight());
    }
    return false;
}

std::string NodeFunctionDiv::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "/" << children[1]->toString();
    return ss.str();
}

NodeFunctionPtr NodeFunctionDiv::deepCopy()
{
    return NodeFunctionDiv::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool NodeFunctionDiv::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return this->getChildren()[0]->as<FunctionNode>()->getStamp()->isNumeric()
        && this->getChildren()[1]->as<FunctionNode>()->getStamp()->isNumeric();
}

}
