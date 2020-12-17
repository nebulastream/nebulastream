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
#include <Operators/LogicalOperators/Arity/UnaryOperatorNode.hpp>

namespace NES {

UnaryOperatorNode::UnaryOperatorNode(OperatorId id)
    : LogicalOperatorNode(id), inputSchema(Schema::create()), outputSchema(Schema::create()) {}

bool UnaryOperatorNode::isBinaryOperator() const { return false; }

bool UnaryOperatorNode::isUnaryOperator() const { return true; }

bool UnaryOperatorNode::isExchangeOperator() const { return false; }

void UnaryOperatorNode::setInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->inputSchema = std::move(inputSchema);
    }
}
void UnaryOperatorNode::setOutputSchema(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}
SchemaPtr UnaryOperatorNode::getInputSchema() const { return inputSchema; }

SchemaPtr UnaryOperatorNode::getOutputSchema() const { return outputSchema; }

bool UnaryOperatorNode::inferSchema() {
    // We assume that all children operators have the same output schema otherwise this plan is not valid
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->inferSchema()) {
            return false;
        }
    }
    if (children.empty()) {
        NES_THROW_RUNTIME_ERROR("UnaryOperatorNode: this node should have at least one child operator");
    }
    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)) {
            NES_ERROR("UnaryOperatorNode: infer schema failed. The schema has to be the same across all child operators.");
            return false;
        }
    }
    inputSchema = childSchema->copy();
    outputSchema = childSchema->copy();
    return true;
}
}// namespace NES
