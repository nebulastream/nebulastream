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

#include <utility>
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionPow.cpp
#include <Functions/ArithmeticalFunctions/NodeFunctionPow.hpp>
#include <Util/Common.hpp>
========
#include <Functions/ArithmeticalFunctions/PowFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/PowFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionPow.cpp
NodeFunctionPow::NodeFunctionPow(DataTypePtr stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Pow") {};

NodeFunctionPow::NodeFunctionPow(NodeFunctionPow* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionPow::create(NodeFunctionPtr const& left, NodeFunctionPtr const& right)
{
    auto powNode = std::make_shared<NodeFunctionPow>(DataTypeFactory::createFloat());
========
PowFunctionNode::PowFunctionNode(DataTypePtr stamp) : ArithmeticalBinaryFunctionNode(std::move(stamp)) {};

PowFunctionNode::PowFunctionNode(PowFunctionNode* other) : ArithmeticalBinaryFunctionNode(other)
{
}

FunctionNodePtr PowFunctionNode::create(FunctionNodePtr const& left, FunctionNodePtr const& right)
{
    auto powNode = std::make_shared<PowFunctionNode>(DataTypeFactory::createFloat());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/PowFunctionNode.cpp
    powNode->setChildren(left, right);
    return powNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionPow.cpp
void NodeFunctionPow::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    NodeFunctionArithmeticalBinary::inferStamp(schema);
========
void PowFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalBinaryFunctionNode::inferStamp(schema);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/PowFunctionNode.cpp

    /// Extend range for POW operation:
    if (stamp->isInteger())
    {
        stamp = DataTypeFactory::createInt64();
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionPow.cpp
        NES_TRACE("NodeFunctionPow: Updated stamp from Integer (assigned in NodeFunctionArithmeticalBinary) to Int64.");
========
        NES_TRACE("PowFunctionNode: Updated stamp from Integer (assigned in ArithmeticalBinaryFunctionNode) to Int64.");
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/PowFunctionNode.cpp
    }
    else if (stamp->isFloat())
    {
        stamp = DataTypeFactory::createDouble();
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionPow.cpp
        NES_TRACE("NodeFunctionPow: Update Float stamp (assigned in NodeFunctionArithmeticalBinary) to Double: {}", toString());
    }
}

bool NodeFunctionPow::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionPow>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionPow>(rhs);
========
        NES_TRACE("PowFunctionNode: Update Float stamp (assigned in ArithmeticalBinaryFunctionNode) to Double: {}", toString());
    }
}

bool PowFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<PowFunctionNode>())
    {
        auto otherAddNode = rhs->as<PowFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/PowFunctionNode.cpp
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionPow.cpp
std::string NodeFunctionPow::toString() const
========
std::string PowFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/PowFunctionNode.cpp
{
    std::stringstream ss;
    ss << "POWER(" << children[0]->toString() << ", " << children[1]->toString() << ")";
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionPow.cpp
NodeFunctionPtr NodeFunctionPow::deepCopy()
{
    return NodeFunctionPow::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}
========
FunctionNodePtr PowFunctionNode::copy()
{
    return PowFunctionNode::create(children[0]->as<FunctionNode>()->copy(), children[1]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/PowFunctionNode.cpp
}
