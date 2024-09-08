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
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAdd.cpp

#include <Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
========
#include <Functions/ArithmeticalFunctions/AddFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AddFunctionNode.cpp
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAdd.cpp
NodeFunctionAdd::NodeFunctionAdd(DataTypePtr stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Add") {};

NodeFunctionAdd::NodeFunctionAdd(NodeFunctionAdd* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionAdd::create(NodeFunctionPtr const& left, NodeFunctionPtr const& right)
{
    auto addNode = std::make_shared<NodeFunctionAdd>(left->getStamp());
========
AddFunctionNode::AddFunctionNode(DataTypePtr stamp) : ArithmeticalBinaryFunctionNode(std::move(stamp)) {};

AddFunctionNode::AddFunctionNode(AddFunctionNode* other) : ArithmeticalBinaryFunctionNode(other)
{
}

FunctionNodePtr AddFunctionNode::create(FunctionNodePtr const& left, FunctionNodePtr const& right)
{
    auto addNode = std::make_shared<AddFunctionNode>(left->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AddFunctionNode.cpp
    addNode->setChildren(left, right);
    return addNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAdd.cpp
bool NodeFunctionAdd::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionAdd>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionAdd>(rhs);
========
bool AddFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<AddFunctionNode>())
    {
        auto otherAddNode = rhs->as<AddFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AddFunctionNode.cpp
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAdd.cpp
std::string NodeFunctionAdd::toString() const
========
std::string AddFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AddFunctionNode.cpp
{
    std::stringstream ss;
    ss << children[0]->toString() << "+" << children[1]->toString();
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAdd.cpp
NodeFunctionPtr NodeFunctionAdd::deepCopy()
{
    return NodeFunctionAdd::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
========
FunctionNodePtr AddFunctionNode::copy()
{
    return AddFunctionNode::create(children[0]->as<FunctionNode>()->copy(), children[1]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AddFunctionNode.cpp
}

}
