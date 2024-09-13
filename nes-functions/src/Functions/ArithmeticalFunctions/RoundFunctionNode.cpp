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
#include <Functions/ArithmeticalFunctions/RoundFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

RoundFunctionNode::RoundFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp), "Round") {};

RoundFunctionNode::RoundFunctionNode(RoundFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr RoundFunctionNode::create(FunctionNodePtr const& child)
{
    auto roundNode = std::make_shared<RoundFunctionNode>(child->getStamp());
    roundNode->setChild(child);
    return roundNode;
}

void RoundFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("RoundFunctionNode: converted stamp to float: {}", toString());
}

bool RoundFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<RoundFunctionNode>())
    {
        auto otherRoundNode = rhs->as<RoundFunctionNode>();
        return child()->equal(otherRoundNode->child());
    }
    return false;
}

std::string RoundFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "ROUND(" << children[0]->toString() << ")";
    return ss.str();
}

FunctionNodePtr RoundFunctionNode::deepCopy()
{
    return RoundFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy());
}

bool RoundFunctionNode::validate() const
{
    NES_NOT_IMPLEMENTED();
}

}
