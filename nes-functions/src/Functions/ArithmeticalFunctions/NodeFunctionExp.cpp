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
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionExp.cpp
#include <Functions/ArithmeticalFunctions/NodeFunctionExp.hpp>
#include <Util/Common.hpp>
========
#include <Functions/ArithmeticalFunctions/ExpFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/ExpFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>


namespace NES
{

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionExp.cpp
NodeFunctionExp::NodeFunctionExp(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Exp") {};

NodeFunctionExp::NodeFunctionExp(NodeFunctionExp* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionExp::create(NodeFunctionPtr const& child)
{
    auto expNode = std::make_shared<NodeFunctionExp>(child->getStamp());
========
ExpFunctionNode::ExpFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp)) {};

ExpFunctionNode::ExpFunctionNode(ExpFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr ExpFunctionNode::create(FunctionNodePtr const& child)
{
    auto expNode = std::make_shared<ExpFunctionNode>(child->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/ExpFunctionNode.cpp
    expNode->setChild(child);
    return expNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionExp.cpp
void NodeFunctionExp::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    NodeFunctionArithmeticalUnary::inferStamp(schema);

    /// change stamp to float with bounds [0, DOUBLE_MAX]. Results of EXP are always positive and become high quickly.
    stamp = DataTypeFactory::createFloat(0.0, std::numeric_limits<double>::max());
    NES_TRACE("NodeFunctionExp: change stamp to float with bounds [0, DOUBLE_MAX]: {}", toString());
}

bool NodeFunctionExp::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionExp>(rhs))
    {
        auto otherExpNode = NES::Util::as<NodeFunctionExp>(rhs);
========
void ExpFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    /// change stamp to float with bounds [0, DOUBLE_MAX]. Results of EXP are always positive and become high quickly.
    stamp = DataTypeFactory::createFloat(0.0, std::numeric_limits<double>::max());
    NES_TRACE("ExpFunctionNode: change stamp to float with bounds [0, DOUBLE_MAX]: {}", toString());
}

bool ExpFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<ExpFunctionNode>())
    {
        auto otherExpNode = rhs->as<ExpFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/ExpFunctionNode.cpp
        return child()->equal(otherExpNode->child());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionExp.cpp
std::string NodeFunctionExp::toString() const
========
std::string ExpFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/ExpFunctionNode.cpp
{
    std::stringstream ss;
    ss << "EXP(" << children[0]->toString() << ")";
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionExp.cpp
NodeFunctionPtr NodeFunctionExp::deepCopy()
{
    return NodeFunctionExp::create(Util::as<NodeFunction>(children[0])->deepCopy());
}
========
FunctionNodePtr ExpFunctionNode::copy()
{
    return ExpFunctionNode::create(children[0]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/ExpFunctionNode.cpp
}
