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
#include <Functions/ArithmeticalFunctions/PowFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
namespace NES
{

PowFunctionNode::PowFunctionNode(DataTypePtr stamp) : ArithmeticalBinaryFunctionNode(std::move(stamp), "Pow") {};

PowFunctionNode::PowFunctionNode(PowFunctionNode* other) : ArithmeticalBinaryFunctionNode(other)
{
}

FunctionNodePtr PowFunctionNode::create(FunctionNodePtr const& left, FunctionNodePtr const& right)
{
    auto powNode = std::make_shared<PowFunctionNode>(DataTypeFactory::createFloat());
    powNode->setChildren(left, right);
    return powNode;
}

void PowFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalBinaryFunctionNode::inferStamp(schema);

    /// Extend range for POW operation:
    if (stamp->isInteger())
    {
        stamp = DataTypeFactory::createInt64();
        NES_TRACE("PowFunctionNode: Updated stamp from Integer (assigned in ArithmeticalBinaryFunctionNode) to Int64.");
    }
    else if (stamp->isFloat())
    {
        stamp = DataTypeFactory::createDouble();
        NES_TRACE("PowFunctionNode: Update Float stamp (assigned in ArithmeticalBinaryFunctionNode) to Double: {}", toString());
    }
}

bool PowFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<PowFunctionNode>())
    {
        auto otherAddNode = rhs->as<PowFunctionNode>();
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string PowFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "POWER(" << children[0]->toString() << ", " << children[1]->toString() << ")";
    return ss.str();
}

FunctionNodePtr PowFunctionNode::deepCopy()
{
    return PowFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool PowFunctionNode::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return this->getChildren()[0]->as<FunctionNode>()->getStamp()->isNumeric() &&
        this->getChildren()[1]->as<FunctionNode>()->getStamp()->isNumeric();
}

}