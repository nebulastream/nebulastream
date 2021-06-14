/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/PowExpressionNode.hpp>
#include <utility>
namespace NES {

PowExpressionNode::PowExpressionNode(DataTypePtr stamp) : ArithmeticalBinaryExpressionNode(std::move(stamp)){};

PowExpressionNode::PowExpressionNode(PowExpressionNode* other) : ArithmeticalBinaryExpressionNode(other) {}

ExpressionNodePtr PowExpressionNode::create(ExpressionNodePtr const& left, ExpressionNodePtr const& right) {
    auto addNode = std::make_shared<PowExpressionNode>(
        DataTypeFactory::createFloat());// TODO: stamp should always be float, but is this the right way?
    addNode->setChildren(left, right);
    return addNode;
}

void PowExpressionNode::inferStamp(SchemaPtr schema) {
    ArithmeticalBinaryExpressionNode::inferStamp(schema);
    if (stamp->isInteger()) {
        stamp = DataTypeFactory::createUInt32();
        NES_DEBUG("PowExpressionNode: Updated stamp from Integer (assigned in ArithmeticalBinaryExpressionNode) to UINT32.");
    } else if (stamp->isFloat()) {
        stamp = DataTypeFactory::
            createDouble();// We could also create an "unsigned double" (as results of pow() is always non-negative), but this is very uncommon in programming languages.
        NES_DEBUG(
            "PowExpressionNode: Updated stamp from Float (assigned in ArithmeticalBinaryExpressionNode::inferStamp) to Double "
            "(FLOAT64).");
    }
}

bool PowExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<PowExpressionNode>()) {
        auto otherAddNode = rhs->as<PowExpressionNode>();
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string PowExpressionNode::toString() const {
    std::stringstream ss;
    ss << "POWER(" << children[0]->toString() << ", " << children[1]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr PowExpressionNode::copy() { return std::make_shared<PowExpressionNode>(PowExpressionNode(this)); }

}// namespace NES