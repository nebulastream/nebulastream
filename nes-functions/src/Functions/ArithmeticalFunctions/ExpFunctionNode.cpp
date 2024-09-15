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
#include <Functions/ArithmeticalFunctions/ExpFunctionNode.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>


namespace NES
{

ExpFunctionNode::ExpFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp), "Exp") {};

ExpFunctionNode::ExpFunctionNode(ExpFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr ExpFunctionNode::create(FunctionNodePtr const& child)
{
    auto expNode = std::make_shared<ExpFunctionNode>(child->getStamp());
    expNode->setChild(child);
    return expNode;
}

void ExpFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    /// change stamp to float with bounds [0, DOUBLE_MAX]. Results of EXP are always positive and become high quickly.
    stamp = DataTypeFactory::createFloat(0.0, std::numeric_limits<double>::max());
    NES_TRACE("ExpFunctionNode: change stamp to float with bounds [0, DOUBLE_MAX]: {}", toString());
}

bool ExpFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<ExpFunctionNode>(rhs))
    {
        auto otherExpNode = NES::Util::as<ExpFunctionNode>(rhs);
        return child()->equal(otherExpNode->child());
    }
    return false;
}

std::string ExpFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "EXP(" << children[0]->toString() << ")";
    return ss.str();
}

FunctionNodePtr ExpFunctionNode::deepCopy()
{
    return ExpFunctionNode::create(Util::as<FunctionNode>(children[0])->deepCopy());
}

bool ExpFunctionNode::validateBeforeLowering() const
{
    if (children.size() != 1)
    {
        return false;
    }
    return this->getChildren()[0]->as<FunctionNode>()->getStamp()->isNumeric();
}

}