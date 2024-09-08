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
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionWhen.cpp
#include <Functions/NodeFunctionWhen.hpp>
#include <Util/Common.hpp>
========
#include <Functions/WhenFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/WhenFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>


namespace NES
{
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionWhen.cpp
NodeFunctionWhen::NodeFunctionWhen(DataTypePtr stamp) : NodeFunctionBinary(std::move(stamp), "When") {};

NodeFunctionWhen::NodeFunctionWhen(NodeFunctionWhen* other) : NodeFunctionBinary(other)
{
}

NodeFunctionPtr NodeFunctionWhen::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto whenNode = std::make_shared<NodeFunctionWhen>(left->getStamp());
========
WhenFunctionNode::WhenFunctionNode(DataTypePtr stamp) : BinaryFunctionNode(std::move(stamp)) {};

WhenFunctionNode::WhenFunctionNode(WhenFunctionNode* other) : BinaryFunctionNode(other)
{
}

FunctionNodePtr WhenFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto whenNode = std::make_shared<WhenFunctionNode>(left->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/WhenFunctionNode.cpp
    whenNode->setChildren(left, right);
    return whenNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionWhen.cpp
void NodeFunctionWhen::inferStamp(SchemaPtr schema)
========
void WhenFunctionNode::inferStamp(SchemaPtr schema)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/WhenFunctionNode.cpp
{
    auto left = getLeft();
    auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    ///left function has to be boolean
    if (!left->getStamp()->isBoolean())
    {
        NES_THROW_RUNTIME_ERROR(
            "Error during stamp inference. Left type needs to be Boolean, but Left was: {} Right was: {}",
            left->getStamp()->toString(),
            right->getStamp()->toString());
    }

    ///set stamp to right stamp, as only the left function will be returned
    stamp = right->getStamp();
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionWhen.cpp
    NES_TRACE("NodeFunctionWhen: we assigned the following stamp: {}", stamp->toString())
}

bool NodeFunctionWhen::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionWhen>(rhs))
    {
        auto otherWhenNode = NES::Util::as<NodeFunctionWhen>(rhs);
========
    NES_TRACE("WhenFunctionNode: we assigned the following stamp: {}", stamp->toString())
}

bool WhenFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<WhenFunctionNode>())
    {
        auto otherWhenNode = rhs->as<WhenFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/WhenFunctionNode.cpp
        return getLeft()->equal(otherWhenNode->getLeft()) && getRight()->equal(otherWhenNode->getRight());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionWhen.cpp
std::string NodeFunctionWhen::toString() const
========
std::string WhenFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/WhenFunctionNode.cpp
{
    std::stringstream ss;
    ss << "WHEN(" << children[0]->toString() << "," << children[1]->toString() << ")";
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionWhen.cpp
NodeFunctionPtr NodeFunctionWhen::deepCopy()
{
    return NodeFunctionWhen::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
========
FunctionNodePtr WhenFunctionNode::copy()
{
    return WhenFunctionNode::create(children[0]->as<FunctionNode>()->copy(), children[1]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/WhenFunctionNode.cpp
}

bool NodeFunctionWhen::validateBeforeLowering() const
{
    /// left function has to be boolean
    const auto functionLeft = this->getLeft();
    return functionLeft->getStamp()->isBoolean();
}

}
