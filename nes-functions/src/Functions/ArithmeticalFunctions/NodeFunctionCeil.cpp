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

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionCeil.cpp
#include <Functions/ArithmeticalFunctions/NodeFunctionCeil.hpp>
#include <Util/Common.hpp>
========
#include <Functions/ArithmeticalFunctions/CeilFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/CeilFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionCeil.cpp
NodeFunctionCeil::NodeFunctionCeil(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Ceil") {};

NodeFunctionCeil::NodeFunctionCeil(NodeFunctionCeil* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionCeil::create(NodeFunctionPtr const& child)
{
    auto ceilNode = std::make_shared<NodeFunctionCeil>(child->getStamp());
========
CeilFunctionNode::CeilFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp)) {};

CeilFunctionNode::CeilFunctionNode(CeilFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr CeilFunctionNode::create(FunctionNodePtr const& child)
{
    auto ceilNode = std::make_shared<CeilFunctionNode>(child->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/CeilFunctionNode.cpp
    ceilNode->setChild(child);
    return ceilNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionCeil.cpp
void NodeFunctionCeil::inferStamp(SchemaPtr schema)
{
    /// infer stamp of the child, check if its numerical, assume the same stamp
    NodeFunctionArithmeticalUnary::inferStamp(schema);

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("NodeFunctionCeil: converted stamp to float: {}", toString());
}

bool NodeFunctionCeil::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionCeil>(rhs))
    {
        auto otherCeilNode = NES::Util::as<NodeFunctionCeil>(rhs);
========
void CeilFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of the child, check if its numerical, assume the same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("CeilFunctionNode: converted stamp to float: {}", toString());
}

bool CeilFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<CeilFunctionNode>())
    {
        auto otherCeilNode = rhs->as<CeilFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/CeilFunctionNode.cpp
        return child()->equal(otherCeilNode->child());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionCeil.cpp
std::string NodeFunctionCeil::toString() const
========
std::string CeilFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/CeilFunctionNode.cpp
{
    std::stringstream ss;
    ss << "CEIL(" << children[0]->toString() << ")";
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionCeil.cpp
NodeFunctionPtr NodeFunctionCeil::deepCopy()
{
    return NodeFunctionCeil::create(Util::as<NodeFunction>(children[0])->deepCopy());
}
========
FunctionNodePtr CeilFunctionNode::copy()
{
    return CeilFunctionNode::create(children[0]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/CeilFunctionNode.cpp
}
