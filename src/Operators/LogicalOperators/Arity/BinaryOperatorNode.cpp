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
#include <Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/Arity/BinaryOperatorNode.hpp>

namespace NES {

BinaryOperatorNode::BinaryOperatorNode(OperatorId id)
    : LogicalOperatorNode(id), leftInputSchema(Schema::create()), rightInputSchema(Schema::create()),
      outputSchema(Schema::create()) {
    //nop
}

bool BinaryOperatorNode::isBinaryOperator() const { return true; }

bool BinaryOperatorNode::isUnaryOperator() const { return false; }

bool BinaryOperatorNode::isExchangeOperator() const { return false; }

void BinaryOperatorNode::setLeftInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->leftInputSchema = std::move(inputSchema);
    }
}

void BinaryOperatorNode::setRightInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->rightInputSchema = std::move(inputSchema);
    }
}
void BinaryOperatorNode::setOutputSchema(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}
SchemaPtr BinaryOperatorNode::getLeftInputSchema() const { return leftInputSchema; }

SchemaPtr BinaryOperatorNode::getRightInputSchema() const { return rightInputSchema; }

SchemaPtr BinaryOperatorNode::getOutputSchema() const { return outputSchema; }

bool BinaryOperatorNode::inferSchema() {

    distinctSchemas.clear();
    //Check the number of child operators
    if (children.size() < 2) {
        NES_ERROR("BinaryOperatorNode: this operator should have at least two child operators");
        throw TypeInferenceException("BinaryOperatorNode: this node should have at least two child operators");
    }

    // Infer schema of all child operators
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->inferSchema()) {
            NES_ERROR("BinaryOperatorNode: failed inferring the schema of the child operator");
            throw TypeInferenceException("BinaryOperatorNode: failed inferring the schema of the child operator");
        }
    }

    //Identify different type of schemas from children operators
    for (auto& child : children) {
        auto childOutputSchema = child->as<OperatorNode>()->getOutputSchema();
        auto found = std::find_if(distinctSchemas.begin(), distinctSchemas.end(), [&](SchemaPtr distinctSchema) {
            return childOutputSchema->equals(distinctSchema, false);
        });
        if (found == distinctSchemas.end()) {
            distinctSchemas.push_back(childOutputSchema);
        }
    }
    return true;
}

}// namespace NES
