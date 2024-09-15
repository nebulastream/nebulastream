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
#include <Functions/ArithmeticalFunctions/FloorFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

FloorFunctionNode::FloorFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp), "Floor") {};

FloorFunctionNode::FloorFunctionNode(FloorFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr FloorFunctionNode::create(FunctionNodePtr const& child)
{
    auto floorNode = std::make_shared<FloorFunctionNode>(child->getStamp());
    floorNode->setChild(child);
    return floorNode;
}

void FloorFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("FloorFunctionNode: converted stamp to float: {}", toString());
}

bool FloorFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<FloorFunctionNode>())
    {
        auto otherFloorNode = rhs->as<FloorFunctionNode>();
        return child()->equal(otherFloorNode->child());
    }
    return false;
}

std::string FloorFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "FLOOR(" << children[0]->toString() << ")";
    return ss.str();
}

FunctionNodePtr FloorFunctionNode::deepCopy()
{
    return FloorFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy());
}

bool FloorFunctionNode::validateBeforeLowering() const
{
    if (children.size() != 1)
    {
        return false;
    }
    return this->getChildren()[0]->as<FunctionNode>()->getStamp()->isNumeric();
}

}