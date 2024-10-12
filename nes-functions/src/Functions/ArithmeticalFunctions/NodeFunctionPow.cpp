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
#include <Functions/ArithmeticalFunctions/NodeFunctionPow.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

NodeFunctionPow::NodeFunctionPow(DataTypePtr stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Pow") {};

NodeFunctionPow::NodeFunctionPow(NodeFunctionPow* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionPow::create(NodeFunctionPtr const& left, NodeFunctionPtr const& right)
{
    auto powNode = std::make_shared<NodeFunctionPow>(DataTypeFactory::createFloat());
    powNode->setChildren(left, right);
    return powNode;
}

void NodeFunctionPow::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    NodeFunctionArithmeticalBinary::inferStamp(schema);

    /// Extend range for POW operation:
    if (stamp->isInteger())
    {
        stamp = DataTypeFactory::createInt64();
        NES_TRACE("NodeFunctionPow: Updated stamp from Integer (assigned in NodeFunctionArithmeticalBinary) to Int64.");
    }
    else if (stamp->isFloat())
    {
        stamp = DataTypeFactory::createDouble();
        NES_TRACE("NodeFunctionPow: Update Float stamp (assigned in NodeFunctionArithmeticalBinary) to Double: {}", toString());
    }
}

bool NodeFunctionPow::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionPow>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionPow>(rhs);
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string NodeFunctionPow::toString() const
{
    std::stringstream ss;
    ss << "POWER(" << children[0]->toString() << ", " << children[1]->toString() << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionPow::deepCopy()
{
bool NodeFunctionPow::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return this->getChildren()[0]->as<FunctionNode>()->getStamp()->isNumeric()
        && this->getChildren()[1]->as<FunctionNode>()->getStamp()->isNumeric();
}

}
