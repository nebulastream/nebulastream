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

#include "API/AttributeField.hpp"

#include "API/Schema.hpp"
#include "Operators/Expressions/FieldAssignmentExpressionNode.hpp"
#include "Operators/LogicalOperators/Operators/LogicalInferModelOperator.hpp"
#include "Util/Logger/Logger.hpp"
#include <filesystem>
#include <utility>

namespace NES::InferModel {

LogicalInferModelOperator::LogicalInferModelOperator(std::string model,
                                                             std::vector<ExpressionNodePtr> inputFields,
                                                             std::vector<ExpressionNodePtr> outputFields,
                                                             OperatorId id)
    : OperatorNode(id), LogicalUnaryOperator(id), model(std::move(model)), inputFields(std::move(inputFields)),
      outputFields(std::move(outputFields)) {
    NES_DEBUG("LogicalInferModelOperator: reading from model {}", this->model);
}

std::string LogicalInferModelOperator::toString() const {
    std::stringstream ss;
    ss << "INFER_MODEL(" << id << ")";
    return ss.str();
}

OperatorNodePtr LogicalInferModelOperator::copy() {
    auto copy = LogicalOperatorFactory::createInferModelOperator(model, inputFields, outputFields, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}
bool LogicalInferModelOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalInferModelOperator>()) {
        auto inferModelOperator = rhs->as<LogicalInferModelOperator>();
        return this->getDeployedModelPath() == inferModelOperator->getDeployedModelPath();
    }
    return false;
}

bool LogicalInferModelOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalInferModelOperator>()->getId() == id;
}

void LogicalInferModelOperator::updateToFullyQualifiedFieldName(FieldAccessExpressionNodePtr field) {
    auto schema = getInputSchema();
    auto fieldName = field->getFieldName();
    auto existingField = schema->getField(fieldName);
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

bool LogicalInferModelOperator::inferSchema() {
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }

    auto inputSchema = getInputSchema();

    for (auto inputField : inputFields) {
        auto inputExpression = inputField->as<FieldAccessExpressionNode>();
        updateToFullyQualifiedFieldName(inputExpression);
        inputExpression->inferStamp( inputSchema);
        auto fieldName = inputExpression->getFieldName();
        inputSchema->replaceField(fieldName, inputExpression->getStamp());
    }

    for (auto outputField : outputFields) {
        auto outputExpression = outputField->as<FieldAccessExpressionNode>();
        updateToFullyQualifiedFieldName(outputExpression);
        auto fieldName = outputExpression->getFieldName();
        if (outputSchema->getField(fieldName)) {
            // The assigned field is part of the current schema.
            // Thus we check if it has the correct type.
            NES_TRACE("Infer Model Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
            outputSchema->replaceField(fieldName, outputExpression->getStamp());
        } else {
            // The assigned field is not part of the current schema.
            // Thus we extend the schema by the new attribute.
            NES_TRACE("Infer Model Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
            outputSchema->addField(fieldName, outputExpression->getStamp());
        }
    }

    return true;
}

void LogicalInferModelOperator::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("InferModelOperatorNode: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty(), "LogicalInferModelOperator: InferModel should have children (?)");
    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << "INFER_MODEL(" + model + ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
const std::string& LogicalInferModelOperator::getModel() const { return model; }

const std::string LogicalInferModelOperator::getDeployedModelPath() const {
    auto idx = model.find_last_of('/');
    auto path = model;

    // If there exist a / in the model path name. If so, then we have to remove the path to only get the file name
    if (idx != std::string::npos) {
        path = model.substr(idx + 1);
    }

    path = std::filesystem::temp_directory_path().string() + "/" + path;
    return path;
}

const std::vector<ExpressionNodePtr>& LogicalInferModelOperator::getInputFields() const { return inputFields; }

const std::vector<ExpressionNodePtr>& LogicalInferModelOperator::getOutputFields() const { return outputFields; }

}// namespace NES::InferModel
