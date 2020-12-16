/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <Operators/LogicalOperators/Arity/ExchangeOperatorNode.hpp>
#include <API/Schema.hpp>

namespace NES {

ExchangeOperatorNode::ExchangeOperatorNode(OperatorId id) : LogicalOperatorNode(id), inputSchema(Schema::create()), outputSchema(Schema::create()) {}

bool ExchangeOperatorNode::isBinaryOperator() { return false; }

bool ExchangeOperatorNode::isUnaryOperator() {
    //TODO: check if this should also be true
    return false;
}

bool ExchangeOperatorNode::isExchangeOperator() { return true; }

void ExchangeOperatorNode::setInputSchema(SchemaPtr inputSchema) { this->inputSchema = std::move(inputSchema); }

void ExchangeOperatorNode::setOutputSchema(SchemaPtr outputSchema) { this->outputSchema = std::move(outputSchema); }

SchemaPtr ExchangeOperatorNode::getInputSchema() const { return inputSchema; }

SchemaPtr ExchangeOperatorNode::getOutputSchema() const { return outputSchema; }


bool ExchangeOperatorNode::inferSchema() {
    // We assume that all children operators have the same output schema otherwise this plan is not valid
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->inferSchema()) {
            return false;
        }
    }
    if (children.empty()) {
        NES_THROW_RUNTIME_ERROR("OperatorNode: this node should have at least one child operator");
    }
    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)) {
            NES_ERROR("OperatorNode: infer schema failed. The schema has to be the same across all child operators.");
            return false;
        }
    }
    inputSchema = childSchema->copy();
    outputSchema = childSchema->copy();
    return true;
}

}// namespace NES
