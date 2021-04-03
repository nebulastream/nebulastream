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
#include <Operators/AbstractOperators/Arity/BinaryOperatorNode.hpp>

namespace NES {

BinaryOperatorNode::BinaryOperatorNode(OperatorId id)
    : OperatorNode(id), leftInputSchema(Schema::create()), rightInputSchema(Schema::create()), outputSchema(Schema::create()) {
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

}// namespace NES
