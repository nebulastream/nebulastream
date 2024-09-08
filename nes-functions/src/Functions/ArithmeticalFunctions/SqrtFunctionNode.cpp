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
#include <Functions/ArithmeticalFunctions/SqrtFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

SqrtFunctionNode::SqrtFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp)) {};

SqrtFunctionNode::SqrtFunctionNode(SqrtFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr SqrtFunctionNode::create(FunctionNodePtr const& child)
{
    auto sqrtNode = std::make_shared<SqrtFunctionNode>(child->getStamp());
    sqrtNode->setChild(child);
    return sqrtNode;
}

void SqrtFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    if ((stamp->isInteger() && DataType::as<Integer>(stamp)->upperBound <= 0)
        || (stamp->isFloat() && DataType::as<Float>(stamp)->upperBound <= 0))
    {
        NES_ERROR("Log10FunctionNode: Non-positive DataType is passed into Log10 function. Arithmetic errors would occur at "
                  "run-time.");
    }

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);

    /// increase lower bound to 0
    stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(stamp, 0.0);
    NES_TRACE("SqrtFunctionNode: converted stamp to float and increased the lower bound of stamp to 0: {}", toString());
}

bool SqrtFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<SqrtFunctionNode>())
    {
        auto otherSqrtNode = rhs->as<SqrtFunctionNode>();
        return child()->equal(otherSqrtNode->child());
    }
    return false;
}

std::string SqrtFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "SQRT(" << children[0]->toString() << ")";
    return ss.str();
}

FunctionNodePtr SqrtFunctionNode::copy()
{
    return SqrtFunctionNode::create(children[0]->as<FunctionNode>()->copy());
}

} /// namespace NES
