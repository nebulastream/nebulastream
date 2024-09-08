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

#include <cmath>
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionFloor.cpp
#include <Functions/ArithmeticalFunctions/NodeFunctionFloor.hpp>
#include <Util/Common.hpp>
========
#include <Functions/ArithmeticalFunctions/FloorFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/FloorFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionFloor.cpp
NodeFunctionFloor::NodeFunctionFloor(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Floor") {};

NodeFunctionFloor::NodeFunctionFloor(NodeFunctionFloor* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionFloor::create(NodeFunctionPtr const& child)
{
    auto floorNode = std::make_shared<NodeFunctionFloor>(child->getStamp());
========
FloorFunctionNode::FloorFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp)) {};

FloorFunctionNode::FloorFunctionNode(FloorFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr FloorFunctionNode::create(FunctionNodePtr const& child)
{
    auto floorNode = std::make_shared<FloorFunctionNode>(child->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/FloorFunctionNode.cpp
    floorNode->setChild(child);
    return floorNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionFloor.cpp
void NodeFunctionFloor::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    NodeFunctionArithmeticalUnary::inferStamp(schema);

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("NodeFunctionFloor: converted stamp to float: {}", toString());
}

bool NodeFunctionFloor::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionFloor>(rhs))
    {
        auto otherFloorNode = NES::Util::as<NodeFunctionFloor>(rhs);
========
void FloorFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("FloorFunctionNode: converted stamp to float: {}", toString());
}

bool FloorFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<FloorFunctionNode>())
    {
        auto otherFloorNode = rhs->as<FloorFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/FloorFunctionNode.cpp
        return child()->equal(otherFloorNode->child());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionFloor.cpp
std::string NodeFunctionFloor::toString() const
========
std::string FloorFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/FloorFunctionNode.cpp
{
    std::stringstream ss;
    ss << "FLOOR(" << children[0]->toString() << ")";
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionFloor.cpp
NodeFunctionPtr NodeFunctionFloor::deepCopy()
{
    return NodeFunctionFloor::create(Util::as<NodeFunction>(children[0])->deepCopy());
}
========
FunctionNodePtr FloorFunctionNode::copy()
{
    return FloorFunctionNode::create(children[0]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/FloorFunctionNode.cpp
}
