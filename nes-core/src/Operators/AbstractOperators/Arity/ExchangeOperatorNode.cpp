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

#include <API/Schema.hpp>
#include <Operators/AbstractOperators/Arity/ExchangeOperatorNode.hpp>

namespace NES {

ExchangeOperatorNode::ExchangeOperatorNode(OperatorId id)
    : OperatorNode(id), inputSchema(Schema::create()), outputSchema(Schema::create()) {}

bool ExchangeOperatorNode::isBinaryOperator() const { return false; }

bool ExchangeOperatorNode::isUnaryOperator() const {
    //TODO: check if this should also be true
    return false;
}

bool ExchangeOperatorNode::isExchangeOperator() const { return true; }

void ExchangeOperatorNode::setInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->inputSchema = std::move(inputSchema);
    }
}

void ExchangeOperatorNode::setOutputSchema(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}

SchemaPtr ExchangeOperatorNode::getInputSchema() const { return inputSchema; }

SchemaPtr ExchangeOperatorNode::getOutputSchema() const { return outputSchema; }

}// namespace NES
