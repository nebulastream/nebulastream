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

#include <API/AttributeField.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>

namespace NES {

InferModelLogicalOperatorNode::InferModelLogicalOperatorNode(std::string model, std::vector<ExpressionItemPtr> inputFields, std::vector<ExpressionItemPtr> outputFields, OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id) {
    this->model = model;
    this->inputFieldsPtr = inputFields;
    this->outputFieldsPtr = outputFields;
}

std::string InferModelLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "Model: " << model << std::endl;
    ss << "input fields:" << std::endl;
    for (auto v : inputFieldsPtr){
        ss << "    " << v->getExpressionNode()->toString() << std::endl;
    }
    ss << "output fields:" << std::endl;
    for (auto v : outputFieldsPtr){
        ss << "    " << v->getExpressionNode()->toString() << std::endl;
    }
    return ss.str();
}

OperatorNodePtr InferModelLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createInferModelOperator(model, inputFieldsPtr, outputFieldsPtr, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setStringSignature(stringSignature);
    copy->setZ3Signature(z3Signature);
    return copy;
}
bool InferModelLogicalOperatorNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<InferModelLogicalOperatorNode>()) {
        auto inferModelOperator = rhs->as<InferModelLogicalOperatorNode>();
        return model == inferModelOperator->model;
    }
    return false;
}

bool InferModelLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<InferModelLogicalOperatorNode>()->getId() == id;
}

void InferModelLogicalOperatorNode::updateToFullyQualifiedFieldName(FieldAccessExpressionNodePtr field){
    auto schema = getInputSchema();
    auto fieldName = field->getFieldName();
    auto existingField = schema->hasFieldName(fieldName);
    if (existingField) {
        field->updateFieldName(existingField->getName());
    } else {
        //Since this is a new field add the stream name from schema
        //Check if field name is already fully qualified
        if (fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos) {
            field->updateFieldName(fieldName);
        } else {
            field->updateFieldName(schema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldName);
        }
    }
}

bool InferModelLogicalOperatorNode::inferSchema() {
    if (!LogicalUnaryOperatorNode::inferSchema()) {
        return false;
    }

    auto inputSchema = getInputSchema();

    for (auto inputField : inputFieldsPtr){
        auto inputExpression = inputField->getExpressionNode()->as<FieldAccessExpressionNode>();
        updateToFullyQualifiedFieldName(inputExpression);
        inputExpression->inferStamp(inputSchema);
        auto fieldName = inputExpression->getFieldName();
        inputSchema->replaceField(fieldName, inputExpression->getStamp());
    }

    for (auto outputField : outputFieldsPtr){
        auto outputExpression = outputField->getExpressionNode()->as<FieldAccessExpressionNode>();
        updateToFullyQualifiedFieldName(outputExpression);
        auto fieldName = outputExpression->getFieldName();
        if (outputSchema->hasFieldName(fieldName)) {
            // The assigned field is part of the current schema.
            // Thus we check if it has the correct type.
            NES_TRACE("MAP Logical Operator: the field " << fieldName << " is already in the schema, so we updated its type.");
            outputSchema->replaceField(fieldName, outputExpression->getStamp());
        } else {
            // The assigned field is not part of the current schema.
            // Thus we extend the schema by the new attribute.
            NES_TRACE("MAP Logical Operator: the field " << fieldName << " is not part of the schema, so we added it.");
            outputSchema->addField(fieldName, outputExpression->getStamp());
        }
    }

    return true;
}

void InferModelLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("InferModelOperatorNode: Inferring String signature for " << operatorNode->toString());
    NES_ASSERT(!children.empty(), "InferModelLogicalOperatorNode: InferModel should have children (?)");
    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    signatureStream << "INFER_MODEL(" + model + ")." << children[0]->as<LogicalOperatorNode>()->getStringSignature();
    setStringSignature(signatureStream.str());
}
const std::string& InferModelLogicalOperatorNode::getModel() const { return model; }

const std::vector<ExpressionItemPtr>& InferModelLogicalOperatorNode::getInputFieldsAsPtr() {
    return inputFieldsPtr;
}
const std::vector<ExpressionItemPtr>& InferModelLogicalOperatorNode::getOutputFieldsAsPtr() {
    return outputFieldsPtr;
}
}// namespace NES
