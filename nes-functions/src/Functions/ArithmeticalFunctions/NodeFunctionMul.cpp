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
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionMul.cpp
#include <Functions/ArithmeticalFunctions/NodeFunctionMul.hpp>
#include <Util/Logger/Logger.hpp>
========
#include <Functions/ArithmeticalFunctions/MulFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/MulFunctionNode.cpp
#include <Common/DataTypes/DataType.hpp>
namespace NES
{
NodeFunctionMul::NodeFunctionMul(DataTypePtr stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Mul") {};

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionMul.cpp
NodeFunctionMul::NodeFunctionMul(NodeFunctionMul* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionMul::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto mulNode = std::make_shared<NodeFunctionMul>(left->getStamp());
========
MulFunctionNode::MulFunctionNode(DataTypePtr stamp) : ArithmeticalBinaryFunctionNode(std::move(stamp)) {};

MulFunctionNode::MulFunctionNode(MulFunctionNode* other) : ArithmeticalBinaryFunctionNode(other)
{
}

FunctionNodePtr MulFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto mulNode = std::make_shared<MulFunctionNode>(left->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/MulFunctionNode.cpp
    mulNode->setChildren(left, right);
    return mulNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionMul.cpp
bool NodeFunctionMul::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionMul>(rhs))
    {
        auto otherMulNode = NES::Util::as<NodeFunctionMul>(rhs);
========
bool MulFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<MulFunctionNode>())
    {
        auto otherMulNode = rhs->as<MulFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/MulFunctionNode.cpp
        return getLeft()->equal(otherMulNode->getLeft()) && getRight()->equal(otherMulNode->getRight());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionMul.cpp
std::string NodeFunctionMul::toString() const
========
std::string MulFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/MulFunctionNode.cpp
{
    std::stringstream ss;
    ss << children[0]->toString() << "*" << children[1]->toString();
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionMul.cpp
NodeFunctionPtr NodeFunctionMul::deepCopy()
{
    return NodeFunctionMul::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
========
FunctionNodePtr MulFunctionNode::copy()
{
    return MulFunctionNode::create(children[0]->as<FunctionNode>()->copy(), children[1]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/MulFunctionNode.cpp
}

}
