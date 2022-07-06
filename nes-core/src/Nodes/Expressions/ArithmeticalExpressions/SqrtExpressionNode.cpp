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
#include <Nodes/Expressions/ArithmeticalExpressions/SqrtExpressionNode.hpp>
#include <cmath>

namespace NES {

SqrtExpressionNode::SqrtExpressionNode(DataTypePtr stamp) : ArithmeticalUnaryExpressionNode(std::move(stamp)){};

SqrtExpressionNode::SqrtExpressionNode(SqrtExpressionNode* other) : ArithmeticalUnaryExpressionNode(other) {}

ExpressionNodePtr SqrtExpressionNode::create(ExpressionNodePtr const& child) {
    auto sqrtNode = std::make_shared<SqrtExpressionNode>(child->getStamp());
    sqrtNode->setChild(child);
    return sqrtNode;
}

void SqrtExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryExpressionNode::inferStamp(schema);

    if ((stamp->isInteger() && DataType::as<Integer>(stamp)->upperBound <= 0)
        || (stamp->isFloat() && DataType::as<Float>(stamp)->upperBound <= 0)) {
        NES_ERROR("Log10ExpressionNode: Non-positive DataType is passed into Log10 expression. Arithmetic errors would occur at "
                  "run-time.");
    }

    // if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);

    // increase lower bound to 0
    stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(stamp, 0.0);
    NES_TRACE("SqrtExpressionNode: converted stamp to float and increased the lower bound of stamp to 0: " << toString());
}

bool SqrtExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<SqrtExpressionNode>()) {
        auto otherSqrtNode = rhs->as<SqrtExpressionNode>();
        return child()->equal(otherSqrtNode->child());
    }
    return false;
}

std::string SqrtExpressionNode::toString() const {
    std::stringstream ss;
    ss << "SQRT(" << children[0]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr SqrtExpressionNode::copy() { return std::make_shared<SqrtExpressionNode>(SqrtExpressionNode(this)); }

}// namespace NES