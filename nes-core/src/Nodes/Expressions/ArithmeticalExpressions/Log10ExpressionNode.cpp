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
#include <Nodes/Expressions/ArithmeticalExpressions/Log10ExpressionNode.hpp>
#include <cmath>

namespace NES {

Log10ExpressionNode::Log10ExpressionNode(DataTypePtr stamp) : ArithmeticalUnaryExpressionNode(std::move(stamp)){};

Log10ExpressionNode::Log10ExpressionNode(Log10ExpressionNode* other) : ArithmeticalUnaryExpressionNode(other) {}

ExpressionNodePtr Log10ExpressionNode::create(ExpressionNodePtr const& child) {
    auto log10Node = std::make_shared<Log10ExpressionNode>(child->getStamp());
    log10Node->setChild(child);
    return log10Node;
}

void Log10ExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryExpressionNode::inferStamp(schema);

    if ((stamp->isInteger() && DataType::as<Integer>(stamp)->upperBound <= 0)
        || (stamp->isFloat() && DataType::as<Float>(stamp)->upperBound <= 0)) {
        NES_ERROR("Log10ExpressionNode: Non-positive DataType is passed into Log10 expression. Arithmetic errors would occur at "
                  "run-time.");
    }

    // Output values can become highly negative for inputs close to +0. Set Double as output stamp.
    stamp = DataTypeFactory::createDouble();
    NES_TRACE("Log10ExpressionNode: set Double as output stamp DataType: " << toString());
}

bool Log10ExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<Log10ExpressionNode>()) {
        auto otherLog10Node = rhs->as<Log10ExpressionNode>();
        return child()->equal(otherLog10Node->child());
    }
    return false;
}

std::string Log10ExpressionNode::toString() const {
    std::stringstream ss;
    ss << "LOG10(" << children[0]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr Log10ExpressionNode::copy() { return std::make_shared<Log10ExpressionNode>(Log10ExpressionNode(this)); }

}// namespace NES