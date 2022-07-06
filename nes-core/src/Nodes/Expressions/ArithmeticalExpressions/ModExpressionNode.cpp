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

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ModExpressionNode.hpp>
namespace NES {

ModExpressionNode::ModExpressionNode(DataTypePtr stamp) : ArithmeticalBinaryExpressionNode(std::move(stamp)){};

ModExpressionNode::ModExpressionNode(ModExpressionNode* other) : ArithmeticalBinaryExpressionNode(other) {}

ExpressionNodePtr ModExpressionNode::create(const ExpressionNodePtr& left, const ExpressionNodePtr& right) {
    auto addNode = std::make_shared<ModExpressionNode>(
        DataTypeFactory::createFloat());// TODO: stamp should always be float, but is this the right way?
    addNode->setChildren(left, right);
    return addNode;
}

void ModExpressionNode::inferStamp(SchemaPtr schema) {
    ArithmeticalBinaryExpressionNode::inferStamp(schema);

    if (stamp->isInteger()) {
        // we know that both children must have been Integer, too
        auto leftAsInt = DataType::as<Integer>(getLeft()->getStamp());
        auto rightAsInt = DataType::as<Integer>(getRight()->getStamp());

        // determine the range of values that the result must be in:
        // e.g. when calculating MOD(..., -128) the result will always be in range [-127, 127]. And no other INT8 divisor will yield a wider range than -128 (=INT8_MIN).
        int64_t range = std::max(std::abs(rightAsInt->lowerBound), std::abs(rightAsInt->upperBound)) - 1;

        // further, we know that the result of MOD(X, ...) can not be larger/smaller than X:
        int64_t newLowerBound = std::max(-range, leftAsInt->lowerBound);
        int64_t newUpperBound = std::min(range, leftAsInt->upperBound);

        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, newLowerBound, newUpperBound);
    } else if (stamp->isFloat()) {
        // children can be integer or float
        auto leftStamp = getLeft()->getStamp();
        auto rightStamp = getRight()->getStamp();

        // target values
        double leftL, leftU, rightL, rightU;

        if (leftStamp->isFloat()) {
            auto leftAsFloat = DataType::as<Float>(static_cast<DataTypePtr>(leftStamp));
            leftL = leftAsFloat->lowerBound;
            leftU = leftAsFloat->upperBound;
            // leftL, leftU = leftAsFloat->lowerBound, leftAsFloat->upperBound;
        } else if (leftStamp->isInteger()) {
            auto leftAsInteger = DataType::as<Integer>(static_cast<DataTypePtr>(leftStamp));
            leftL = (double) leftAsInteger->lowerBound;
            leftU = (double) leftAsInteger->upperBound;
            // leftL, leftU = leftAsFloat->lowerBound, leftAsFloat->upperBound;
        } else {
            return;
        }

        if (rightStamp->isFloat()) {
            auto rightAsFloat = DataType::as<Float>(static_cast<DataTypePtr>(rightStamp));
            rightL = rightAsFloat->lowerBound;
            rightU = rightAsFloat->upperBound;
            // rightL, rightU = rightAsFloat->lowerBound, rightAsFloat->upperBound;
        } else if (rightStamp->isInteger()) {
            auto rightAsInteger = DataType::as<Integer>(static_cast<DataTypePtr>(rightStamp));
            rightL = (double) rightAsInteger->lowerBound;
            rightU = (double) rightAsInteger->upperBound;
            // rightL, rightU = rightAsFloat->lowerBound, rightAsFloat->upperBound;
        } else {
            return;
        }

        double range = std::max(std::abs(rightL), std::abs(rightU));
        double newLowerBound = std::max(-range, leftL);
        double newUpperBound = std::min(range, leftU);

        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, newLowerBound, newUpperBound);
    }
    // do nothing if the stamp is of type undefined (from ArithmeticalBinaryExpressionNode::inferStamp(schema);)
}

bool ModExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<ModExpressionNode>()) {
        auto otherAddNode = rhs->as<ModExpressionNode>();
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string ModExpressionNode::toString() const {
    std::stringstream ss;
    ss << children[0]->toString() << "%" << children[1]->toString();
    return ss.str();
}

ExpressionNodePtr ModExpressionNode::copy() { return std::make_shared<ModExpressionNode>(ModExpressionNode(this)); }

}// namespace NES