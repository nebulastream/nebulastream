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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <utility>

namespace NES {

LogicalProjectionOperator::LogicalProjectionOperator(std::vector<ExpressionNodePtr> expressions, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), expressions(std::move(expressions)) {}

std::vector<ExpressionNodePtr> LogicalProjectionOperator::getExpressions() const { return expressions; }

bool LogicalProjectionOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalProjectionOperator>()->getId() == id;
}

bool LogicalProjectionOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalProjectionOperator>()) {
        auto projection = rhs->as<LogicalProjectionOperator>();
        return outputSchema->equals(projection->outputSchema);
    }
    return false;
};

std::string LogicalProjectionOperator::toString() const {
    std::stringstream ss;
    ss << "PROJECTION(" << id << ", schema=" << outputSchema->toString() << ")";
    return ss.str();
}

bool LogicalProjectionOperator::inferSchema() {
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }
    NES_DEBUG("proj input={}  outputSchema={} this proj={}", inputSchema->toString(), outputSchema->toString(), toString());
    outputSchema->clear();
    for (const auto& expression : expressions) {

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
            NES_ERROR("LogicalProjectionOperator: Expression has to be an FieldAccessExpression or a FieldRenameExpression "
                      "but it was a {}",
                      expression->toString());
            throw TypeInferenceException("LogicalProjectionOperator: Expression has to be an FieldAccessExpression or a "
                                         "FieldRenameExpression but it was a "
                                         + expression->toString());
        }
    }
    return true;
}

OperatorPtr LogicalProjectionOperator::copy() {
    std::vector<ExpressionNodePtr> copyOfProjectionExpressions;
    for (const auto& originalExpression : expressions) {
        copyOfProjectionExpressions.emplace_back(originalExpression->copy());
    }
    auto copy = LogicalOperatorFactory::createProjectionOperator(copyOfProjectionExpressions, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    copy->setStatisticId(statisticId);
    copy->setHashBasedSignature(hashBasedSignature);
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalProjectionOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("LogicalProjectionOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty(), "LogicalProjectionOperator: Project should have children.");
    //Infer query signatures for child operators
    for (const auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    std::vector<std::string> fields;
    for (const auto& field : outputSchema->fields) {
        fields.push_back(field->getName());
    }
    std::sort(fields.begin(), fields.end());
    signatureStream << "PROJECTION(";
    for (const auto& field : fields) {
        signatureStream << " " << field << " ";
    }
    auto childSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES
