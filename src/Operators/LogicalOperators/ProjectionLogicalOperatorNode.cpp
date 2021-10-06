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
#include <API/AttributeField.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES {

ProjectionLogicalOperatorNode::ProjectionLogicalOperatorNode(std::vector<ExpressionNodePtr> expressions, uint64_t id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), expressions(std::move(expressions)) {}

std::vector<ExpressionNodePtr> ProjectionLogicalOperatorNode::getExpressions() { return expressions; }

bool ProjectionLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<ProjectionLogicalOperatorNode>()->getId() == id;
}

bool ProjectionLogicalOperatorNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<ProjectionLogicalOperatorNode>()) {
        auto projection = rhs->as<ProjectionLogicalOperatorNode>();
        return outputSchema->equals(projection->outputSchema);
    }
    return false;
};

std::string ProjectionLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "PROJECTION(" << id << ", schema=" << outputSchema->toString() << ")";
    return ss.str();
}

bool ProjectionLogicalOperatorNode::inferSchema() {
    if (!LogicalUnaryOperatorNode::inferSchema()) {
        return false;
    }
    NES_DEBUG("proj input=" << inputSchema->toString() << " outputSchema=" << outputSchema->toString()
                            << " this proj=" << toString());
    outputSchema->clear();
    for (auto& expression : expressions) {

        //Infer schema of the field expression
        expression->inferStamp(inputSchema);

        // Build the output schema
        if (expression->instanceOf<FieldRenameExpressionNode>()) {
            auto fieldRename = expression->as<FieldRenameExpressionNode>();
            outputSchema->addField(fieldRename->getNewFieldName(), fieldRename->getStamp());
        } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
            auto fieldAccess = expression->as<FieldAccessExpressionNode>();
            outputSchema->addField(fieldAccess->getFieldName(), fieldAccess->getStamp());
        } else {
            NES_ERROR("ProjectionLogicalOperatorNode: Expression has to be an FieldAccessExpression or a FieldRenameExpression "
                      "but it was a "
                      + expression->toString());
            throw TypeInferenceException("ProjectionLogicalOperatorNode: Expression has to be an FieldAccessExpression or a "
                                         "FieldRenameExpression but it was a "
                                         + expression->toString());
        }
    }
    return true;
}

OperatorNodePtr ProjectionLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createProjectionOperator(expressions, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

void ProjectionLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("ProjectionLogicalOperatorNode: Inferring String signature for " << operatorNode->toString());
    NES_ASSERT(!children.empty(), "ProjectionLogicalOperatorNode: Project should have children.");
    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    std::vector<std::string> fields;
    for (auto& field : outputSchema->fields) {
        fields.push_back(field->getName());
    }
    std::sort(fields.begin(), fields.end());
    signatureStream << "PROJECTION(";
    for (const auto& field : fields) {
        signatureStream << " " << field << " ";
    }
    signatureStream << ")." << children[0]->as<LogicalOperatorNode>()->getStringSignature();
    setStringSignature(signatureStream.str());
}
}// namespace NES
