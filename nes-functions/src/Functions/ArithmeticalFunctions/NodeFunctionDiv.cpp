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
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionDiv.cpp
#include <Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp>
#include <Util/Logger/Logger.hpp>
========
#include <Functions/ArithmeticalFunctions/DivFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/DivFunctionNode.cpp
#include <Common/DataTypes/DataType.hpp>
namespace NES
{

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionDiv.cpp
NodeFunctionDiv::NodeFunctionDiv(DataTypePtr stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Div") {};

NodeFunctionDiv::NodeFunctionDiv(NodeFunctionDiv* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionDiv::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto divNode = std::make_shared<NodeFunctionDiv>(left->getStamp());
========
DivFunctionNode::DivFunctionNode(DataTypePtr stamp) : ArithmeticalBinaryFunctionNode(std::move(stamp)) {};

DivFunctionNode::DivFunctionNode(DivFunctionNode* other) : ArithmeticalBinaryFunctionNode(other)
{
}

FunctionNodePtr DivFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto divNode = std::make_shared<DivFunctionNode>(left->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/DivFunctionNode.cpp
    divNode->setChildren(left, right);
    return divNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionDiv.cpp
bool NodeFunctionDiv::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionDiv>(rhs))
    {
        auto otherDivNode = NES::Util::as<NodeFunctionDiv>(rhs);
========
bool DivFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<DivFunctionNode>())
    {
        auto otherDivNode = rhs->as<DivFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/DivFunctionNode.cpp
        return getLeft()->equal(otherDivNode->getLeft()) && getRight()->equal(otherDivNode->getRight());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionDiv.cpp
std::string NodeFunctionDiv::toString() const
========
std::string DivFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/DivFunctionNode.cpp
{
    std::stringstream ss;
    ss << children[0]->toString() << "/" << children[1]->toString();
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionDiv.cpp
NodeFunctionPtr NodeFunctionDiv::deepCopy()
{
    return NodeFunctionDiv::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
========
FunctionNodePtr DivFunctionNode::copy()
{
    return DivFunctionNode::create(children[0]->as<FunctionNode>()->copy(), children[1]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/DivFunctionNode.cpp
}


}
