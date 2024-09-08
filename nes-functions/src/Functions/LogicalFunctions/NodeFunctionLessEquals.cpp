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

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionLessEquals.cpp
#include <Functions/LogicalFunctions/NodeFunctionLessEquals.hpp>
#include <Util/Common.hpp>
========
#include <Functions/LogicalFunctions/LessEqualsFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/LessEqualsFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionLessEquals.cpp

NodeFunctionLessEquals::NodeFunctionLessEquals() : NodeFunctionLogicalBinary("LessEquals")
{
}

NodeFunctionLessEquals::NodeFunctionLessEquals(NodeFunctionLessEquals* other) : NodeFunctionLogicalBinary(other)
{
}

NodeFunctionPtr NodeFunctionLessEquals::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto lessThen = std::make_shared<NodeFunctionLessEquals>();
========
LessEqualsFunctionNode::LessEqualsFunctionNode(LessEqualsFunctionNode* other) : LogicalBinaryFunctionNode(other)
{
}

FunctionNodePtr LessEqualsFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto lessThen = std::make_shared<LessEqualsFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/LessEqualsFunctionNode.cpp
    lessThen->setChildren(left, right);
    return lessThen;
}

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionLessEquals.cpp
bool NodeFunctionLessEquals::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionLessEquals>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionLessEquals>(rhs);
========
bool LessEqualsFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<LessEqualsFunctionNode>())
    {
        auto other = rhs->as<LessEqualsFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/LessEqualsFunctionNode.cpp
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionLessEquals.cpp
std::string NodeFunctionLessEquals::toString() const
========
std::string LessEqualsFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/LessEqualsFunctionNode.cpp
{
    std::stringstream ss;
    ss << children[0]->toString() << "<=" << children[1]->toString();
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/LogicalFunctions/NodeFunctionLessEquals.cpp
NodeFunctionPtr NodeFunctionLessEquals::deepCopy()
{
    return NodeFunctionLessEquals::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
========
FunctionNodePtr LessEqualsFunctionNode::copy()
{
    return LessEqualsFunctionNode::create(children[0]->as<FunctionNode>()->copy(), children[1]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/LogicalFunctions/LessEqualsFunctionNode.cpp
}

}
