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
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAbs.cpp
#include <Functions/ArithmeticalFunctions/NodeFunctionAbs.hpp>
#include <Util/Common.hpp>
========
#include <Functions/ArithmeticalFunctions/AbsFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AbsFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAbs.cpp
NodeFunctionAbs::NodeFunctionAbs(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Abs") {};

NodeFunctionAbs::NodeFunctionAbs(NodeFunctionAbs* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionAbs::create(const NodeFunctionPtr& child)
{
    auto absNode = std::make_shared<NodeFunctionAbs>(child->getStamp());
========
AbsFunctionNode::AbsFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp)) {};

AbsFunctionNode::AbsFunctionNode(AbsFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr AbsFunctionNode::create(const FunctionNodePtr& child)
{
    auto absNode = std::make_shared<AbsFunctionNode>(child->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AbsFunctionNode.cpp
    absNode->setChild(child);
    return absNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAbs.cpp
void NodeFunctionAbs::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    NodeFunctionArithmeticalUnary::inferStamp(schema);

    /// increase lower bound to 0
    stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(stamp, 0_s64);
    NES_TRACE("NodeFunctionAbs: increased the lower bound of stamp to 0: {}", toString());
}

bool NodeFunctionAbs::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionAbs>(rhs))
    {
        auto otherAbsNode = NES::Util::as<NodeFunctionAbs>(rhs);
========
void AbsFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    /// increase lower bound to 0
    stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(stamp, 0_s64);
    NES_TRACE("AbsFunctionNode: increased the lower bound of stamp to 0: {}", toString());
}

bool AbsFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<AbsFunctionNode>())
    {
        auto otherAbsNode = rhs->as<AbsFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AbsFunctionNode.cpp
        return child()->equal(otherAbsNode->child());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAbs.cpp
std::string NodeFunctionAbs::toString() const
========
std::string AbsFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AbsFunctionNode.cpp
{
    std::stringstream ss;
    ss << "ABS(" << children[0]->toString() << ")";
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionAbs.cpp
NodeFunctionPtr NodeFunctionAbs::deepCopy()
{
    return NodeFunctionAbs::create(Util::as<NodeFunction>(children[0])->deepCopy());
========
FunctionNodePtr AbsFunctionNode::copy()
{
    return AbsFunctionNode::create(children[0]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/AbsFunctionNode.cpp
}

}
