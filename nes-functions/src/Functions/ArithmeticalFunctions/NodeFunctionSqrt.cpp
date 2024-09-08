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
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionSqrt.cpp
#include <Functions/ArithmeticalFunctions/NodeFunctionSqrt.hpp>
#include <Util/Common.hpp>
========
#include <Functions/ArithmeticalFunctions/SqrtFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/SqrtFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>


namespace NES
{

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionSqrt.cpp
NodeFunctionSqrt::NodeFunctionSqrt(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Sqrt") {};

NodeFunctionSqrt::NodeFunctionSqrt(NodeFunctionSqrt* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionSqrt::create(NodeFunctionPtr const& child)
{
    auto sqrtNode = std::make_shared<NodeFunctionSqrt>(child->getStamp());
========
SqrtFunctionNode::SqrtFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp)) {};

SqrtFunctionNode::SqrtFunctionNode(SqrtFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr SqrtFunctionNode::create(FunctionNodePtr const& child)
{
    auto sqrtNode = std::make_shared<SqrtFunctionNode>(child->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/SqrtFunctionNode.cpp
    sqrtNode->setChild(child);
    return sqrtNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionSqrt.cpp
void NodeFunctionSqrt::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    NodeFunctionArithmeticalUnary::inferStamp(schema);
========
void SqrtFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/SqrtFunctionNode.cpp

    if ((stamp->isInteger() && DataType::as<Integer>(stamp)->upperBound <= 0)
        || (stamp->isFloat() && DataType::as<Float>(stamp)->upperBound <= 0))
    {
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionSqrt.cpp
        NES_ERROR("Log10NodeFunction: Non-positive DataType is passed into Log10 function. Arithmetic errors would occur at "
========
        NES_ERROR("Log10FunctionNode: Non-positive DataType is passed into Log10 function. Arithmetic errors would occur at "
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/SqrtFunctionNode.cpp
                  "run-time.");
    }

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);

    /// increase lower bound to 0
    stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(stamp, 0.0);
<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionSqrt.cpp
    NES_TRACE("NodeFunctionSqrt: converted stamp to float and increased the lower bound of stamp to 0: {}", toString());
}

bool NodeFunctionSqrt::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionSqrt>(rhs))
    {
        auto otherSqrtNode = NES::Util::as<NodeFunctionSqrt>(rhs);
========
    NES_TRACE("SqrtFunctionNode: converted stamp to float and increased the lower bound of stamp to 0: {}", toString());
}

bool SqrtFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<SqrtFunctionNode>())
    {
        auto otherSqrtNode = rhs->as<SqrtFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/SqrtFunctionNode.cpp
        return child()->equal(otherSqrtNode->child());
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionSqrt.cpp
std::string NodeFunctionSqrt::toString() const
========
std::string SqrtFunctionNode::toString() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/SqrtFunctionNode.cpp
{
    std::stringstream ss;
    ss << "SQRT(" << children[0]->toString() << ")";
    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/ArithmeticalFunctions/NodeFunctionSqrt.cpp
NodeFunctionPtr NodeFunctionSqrt::deepCopy()
{
    return NodeFunctionSqrt::create(Util::as<NodeFunction>(children[0])->deepCopy());
========
FunctionNodePtr SqrtFunctionNode::copy()
{
    return SqrtFunctionNode::create(children[0]->as<FunctionNode>()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/ArithmeticalFunctions/SqrtFunctionNode.cpp
}

}
