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
#include <Operators/AbstractOperators/Arity/BinaryOperator.hpp>

namespace NES {

BinaryOperator::BinaryOperator(OperatorId id)
    : Operator(id), leftInputSchema(Schema::create()), rightInputSchema(Schema::create()), outputSchema(Schema::create()) {
    //nop
}

void BinaryOperator::setLeftInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->leftInputSchema = std::move(inputSchema);
    }
}

void BinaryOperator::setRightInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->rightInputSchema = std::move(inputSchema);
    }
}
void BinaryOperator::setOutputSchema(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}
SchemaPtr BinaryOperator::getLeftInputSchema() const { return leftInputSchema; }

SchemaPtr BinaryOperator::getRightInputSchema() const { return rightInputSchema; }

SchemaPtr BinaryOperator::getOutputSchema() const { return outputSchema; }

std::vector<OriginId> BinaryOperator::getLeftInputOriginIds() { return leftInputOriginIds; }

std::vector<OriginId> BinaryOperator::getAllInputOriginIds() {
    std::vector<OriginId> vec;
    vec.insert(vec.end(), leftInputOriginIds.begin(), leftInputOriginIds.end());
    vec.insert(vec.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());
    return vec;
}

void BinaryOperator::setLeftInputOriginIds(std::vector<OriginId> originIds) { this->leftInputOriginIds = originIds; }

std::vector<OriginId> BinaryOperator::getRightInputOriginIds() { return rightInputOriginIds; }

void BinaryOperator::setRightInputOriginIds(std::vector<OriginId> originIds) { this->rightInputOriginIds = originIds; }

const std::vector<OriginId> BinaryOperator::getOutputOriginIds() const {
    std::vector<uint64_t> outputOriginIds = leftInputOriginIds;
    outputOriginIds.insert(outputOriginIds.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());
    return outputOriginIds;
}

std::string BinaryOperator::toString() const {
    std::stringstream out;
    out << Operator::toString();
    out << "leftInputSchema: " << leftInputSchema->toString() << "\n";
    out << "rightInputSchema: " << rightInputSchema->toString() << "\n";
    out << "outputSchema: " << outputSchema->toString() << "\n";
    out << "distinctSchemas: " << Util::concatenateVectorAsString<SchemaPtr>(distinctSchemas);
    out << "leftInputOriginIds: " << Util::concatenateVectorAsString<uint64_t>(leftInputOriginIds);
    out << "rightInputOriginIds: " << Util::concatenateVectorAsString<uint64_t>(rightInputOriginIds);
    return out.str();
}

}// namespace NES
