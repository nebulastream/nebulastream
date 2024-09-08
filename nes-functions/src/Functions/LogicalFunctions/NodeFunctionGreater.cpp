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

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionGreater.cpp
#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Util/Common.hpp>
========
#include <Functions/LogicalFunctions/GreaterFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/GreaterFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionGreater.cpp

NodeFunctionGreater::NodeFunctionGreater() noexcept : NodeFunctionLogicalBinary("Greater")
{
}

NodeFunctionGreater::NodeFunctionGreater(NodeFunctionGreater* other) : NodeFunctionLogicalBinary(other)
{
}

NodeFunctionPtr NodeFunctionGreater::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto greater = std::make_shared<NodeFunctionGreater>();
========
GreaterFunctionNode::GreaterFunctionNode(GreaterFunctionNode* other) : LogicalBinaryFunctionNode(other)
{
}

FunctionNodePtr GreaterFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto greater = std::make_shared<GreaterFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/GreaterFunctionNode.cpp
    greater->setChildren(left, right);
    return greater;
}

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionGreater.cpp
bool NodeFunctionGreater::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionGreater>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionGreater>(rhs);
========
bool GreaterFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<GreaterFunctionNode>())
    {
        auto other = rhs->as<GreaterFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/GreaterFunctionNode.cpp
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionGreater.cpp
std::string NodeFunctionGreater::toString() const
========
std::string GreaterFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/GreaterFunctionNode.cpp
{
    std::stringstream ss;
    ss << children[0]->toString() << ">" << children[1]->toString();
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionGreater.cpp
NodeFunctionPtr NodeFunctionGreater::deepCopy()
{
    return NodeFunctionGreater::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
========
FunctionNodePtr GreaterFunctionNode::copy()
{
    return GreaterFunctionNode::create(children[0]->as<FunctionNode>()->copy(), children[1]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/GreaterFunctionNode.cpp
}

}
