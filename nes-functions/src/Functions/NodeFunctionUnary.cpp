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
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionUnary.cpp
#include <Functions/NodeFunctionUnary.hpp>
#include <Util/Common.hpp>

namespace NES
{
NodeFunctionUnary::NodeFunctionUnary(DataTypePtr stamp, std::string name) : NodeFunction(std::move(stamp), std::move(name))
{
}

NodeFunctionUnary::NodeFunctionUnary(NodeFunctionUnary* other) : NodeFunction(other)
{
}

void NodeFunctionUnary::setChild(const NodeFunctionPtr& child)
{
    addChildWithEqual(child);
}
NodeFunctionPtr NodeFunctionUnary::child() const
{
    return Util::as<NodeFunction>(children[0]);
}
========
#include <Functions/UnaryFunctionNode.hpp>

namespace NES
{
UnaryFunctionNode::UnaryFunctionNode(DataTypePtr stamp) : FunctionNode(std::move(stamp))
{
}

UnaryFunctionNode::UnaryFunctionNode(UnaryFunctionNode* other) : FunctionNode(other)
{
}

void UnaryFunctionNode::setChild(const FunctionNodePtr& child)
{
    addChildWithEqual(child);
}
FunctionNodePtr UnaryFunctionNode::child() const
{
    return children[0]->as<FunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/UnaryFunctionNode.cpp
}
