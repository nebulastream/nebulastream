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

#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Util/Logger.hpp>

namespace NES {

ProjectionLogicalOperatorNode::ProjectionLogicalOperatorNode(std::vector<ExpressionItem> expressions, uint64_t id)
    : expressions(expressions), UnaryOperatorNode(id) {}

bool ProjectionLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<ProjectionLogicalOperatorNode>()->getId() == id;
}

bool ProjectionLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<ProjectionLogicalOperatorNode>()) {
        auto projection = rhs->as<ProjectionLogicalOperatorNode>();
        return outputSchema->equals(projection->outputSchema);
    }
    return false;
};

const std::string ProjectionLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "PROJECTION(" << id << ", schema=" << outputSchema->toString() << ")";
    return ss.str();
}

bool ProjectionLogicalOperatorNode::inferSchema() {
    UnaryOperatorNode::inferSchema();
    outputSchema = Schema::create();
    for (auto& exp : expressions) {
        auto expression = exp.getExpressionNode();
        if (!expression->instanceOf<FieldAccessExpressionNode>()) {
            NES_ERROR("Query: stream has to be an FieldAccessExpression but it was a " + expression->toString());
        }
        auto fieldAccess = expression->as<FieldAccessExpressionNode>();
        if (inputSchema->contains(fieldAccess->getFieldName())) {
            outputSchema->addField(inputSchema->get(fieldAccess->getFieldName()));
        } else {
            NES_ERROR("ProjectionLogicalOperatorNode::inferSchema(): expression not found=" << fieldAccess->getFieldName());
            return false;
        }
    }
    return true;
}

std::vector<ExpressionItem> ProjectionLogicalOperatorNode::getExpressions() { return expressions; }

OperatorNodePtr ProjectionLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createProjectionOperator(expressions, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
}// namespace NES
