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

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionLogicalUnary.cpp
#include <Functions/LogicalFunctions/NodeFunctionLogicalUnary.hpp>
#include <Util/Common.hpp>
========
#include <Functions/LogicalFunctions/LogicalUnaryFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/LogicalUnaryFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>


namespace NES
{

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionLogicalUnary.cpp
NodeFunctionLogicalUnary::NodeFunctionLogicalUnary(std::string name)
    : NodeFunctionUnary(DataTypeFactory::createBoolean(), std::move(name)), LogicalNodeFunction()
{
}

NodeFunctionLogicalUnary::NodeFunctionLogicalUnary(NodeFunctionLogicalUnary* other) : NodeFunctionUnary(other)
{
}

bool NodeFunctionLogicalUnary::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionLogicalUnary>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionLogicalUnary>(rhs);
========
LogicalUnaryFunctionNode::LogicalUnaryFunctionNode() : UnaryFunctionNode(DataTypeFactory::createBoolean()), LogicalFunctionNode()
{
}

LogicalUnaryFunctionNode::LogicalUnaryFunctionNode(LogicalUnaryFunctionNode* other) : UnaryFunctionNode(other)
{
}

bool LogicalUnaryFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<LogicalUnaryFunctionNode>())
    {
        auto other = rhs->as<LogicalUnaryFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/LogicalUnaryFunctionNode.cpp
        return this->getChildren()[0]->equal(other->getChildren()[0]);
    }
    return false;
}
}
