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

#include <Util/OperatorsUtil.hpp>
#include <API/Schema.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>

namespace NES {

UnaryOperatorNode::UnaryOperatorNode(OperatorId id)
    : OperatorNode(id), inputSchema(Schema::create()), outputSchema(Schema::create()) {}

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

void UnaryOperatorNode::setInputOriginIds(std::vector<OriginId> originIds) { this->inputOriginIds = originIds; }

const std::vector<OriginId> UnaryOperatorNode::getInputOriginIds() const { return inputOriginIds; }

const std::vector<OriginId> UnaryOperatorNode::getOutputOriginIds() const { return inputOriginIds; }

std::string UnaryOperatorNode::toString() const {
    std::stringstream out;
    out << OperatorNode::toString();
    out << "inputSchema: " << inputSchema->toString() << "\n";
    out << "outputSchema: " << outputSchema->toString() << "\n";
    out << "inputOriginIds: " << Util::concatenateVectorAsString<uint64_t>(inputOriginIds);
    return out.str();
}

}// namespace NES
