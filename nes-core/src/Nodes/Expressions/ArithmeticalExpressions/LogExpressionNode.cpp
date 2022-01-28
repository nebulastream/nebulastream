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
#include <Nodes/Expressions/ArithmeticalExpressions/LogExpressionNode.hpp>
#include <cmath>

namespace NES {

LogExpressionNode::LogExpressionNode(DataTypePtr stamp) : ArithmeticalUnaryExpressionNode(std::move(stamp)){};

LogExpressionNode::LogExpressionNode(LogExpressionNode* other) : ArithmeticalUnaryExpressionNode(other) {}

ExpressionNodePtr LogExpressionNode::create(ExpressionNodePtr const& child) {
    auto logNode = std::make_shared<LogExpressionNode>(child->getStamp());
    logNode->setChild(child);
    return logNode;
}

void LogExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryExpressionNode::inferStamp(schema);

    if ((stamp->isInteger() && DataType::as<Integer>(stamp)->upperBound <= 0)
        || (stamp->isFloat() && DataType::as<Float>(stamp)->upperBound <= 0)) {
        NES_ERROR("Log10ExpressionNode: Non-positive DataType is passed into Log10 expression. Arithmetic errors would occur at "
                  "run-time.");
    }

    // Output values can become highly negative for inputs close to +0. Set Double as output stamp.
    stamp = DataTypeFactory::createDouble();
    NES_TRACE("LogExpressionNode: set Double as output stamp DataType: " << toString());
}

bool LogExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogExpressionNode>()) {
        auto otherLogNode = rhs->as<LogExpressionNode>();
        return child()->equal(otherLogNode->child());
    }
    return false;
}

std::string LogExpressionNode::toString() const {
    std::stringstream ss;
    ss << "LOGN(" << children[0]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr LogExpressionNode::copy() { return std::make_shared<LogExpressionNode>(LogExpressionNode(this)); }

}// namespace NES