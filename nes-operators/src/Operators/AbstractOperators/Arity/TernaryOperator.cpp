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
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Operators/AbstractOperators/Arity/TernaryOperator.hpp>
#include <Util/OperatorsUtil.hpp>
#include <fmt/format.h>

namespace NES {

TernaryOperator::TernaryOperator(OperatorId id) : Operator(id) {}

void TernaryOperator::setLeftInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->leftInputSchema = std::move(inputSchema);
    }
}

void TernaryOperator::setMiddleInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->middleInputSchema = std::move(inputSchema);
    }
}

void TernaryOperator::setRightInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->rightInputSchema = std::move(inputSchema);
    }
}
void TernaryOperator::setOutputSchema(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}
SchemaPtr TernaryOperator::getLeftInputSchema() const { return leftInputSchema; }

SchemaPtr TernaryOperator::getMiddleInputSchema() const { return middleInputSchema; }

SchemaPtr TernaryOperator::getRightInputSchema() const { return rightInputSchema; }

SchemaPtr TernaryOperator::getOutputSchema() const { return outputSchema; }

std::vector<OriginId> TernaryOperator::getLeftInputOriginIds() { return leftInputOriginIds; }

std::vector<OriginId> TernaryOperator::getMiddleInputOriginIds() { return middleInputOriginIds; }

std::vector<OriginId> TernaryOperator::getRightInputOriginIds() { return rightInputOriginIds; }

std::vector<OriginId> TernaryOperator::getAllInputOriginIds() {
    std::vector<OriginId> vec;
    vec.insert(vec.end(), leftInputOriginIds.begin(), leftInputOriginIds.end());
    vec.insert(vec.end(), middleInputOriginIds.begin(), middleInputOriginIds.end());
    vec.insert(vec.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());
    return vec;
}

void TernaryOperator::setLeftInputOriginIds(const std::vector<OriginId>& originIds) { this->leftInputOriginIds = originIds; }

void TernaryOperator::setMiddleInputOriginIds(const std::vector<OriginId>& originIds) { this->middleInputOriginIds = originIds; }

void TernaryOperator::setRightInputOriginIds(const std::vector<OriginId>& originIds) { this->rightInputOriginIds = originIds; }

std::vector<OriginId> TernaryOperator::getOutputOriginIds() const {
    std::vector<OriginId> outputOriginIds = leftInputOriginIds;
    outputOriginIds.insert(outputOriginIds.end(), middleInputOriginIds.begin(), middleInputOriginIds.end());
    outputOriginIds.insert(outputOriginIds.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());
    return outputOriginIds;
}

std::string TernaryOperator::toString() const {
    return fmt::format("leftInputSchema: {}\n"
                       "middleInputSchema: {}\n"
                       "rightInputSchema: {}\n"
                       "outputSchema: {}\n"
                       "distinctSchemas: {}\n"
                       "leftInputOriginIds: {}\n"
                       "rightInputOriginIds: {}",
                       leftInputSchema->toString(),
                       middleInputSchema->toString(),
                       rightInputSchema->toString(),
                       outputSchema->toString(),
                       Util::concatenateVectorAsString(distinctSchemas),
                       fmt::join(leftInputOriginIds.begin(), leftInputOriginIds.end(), ", "),
                       fmt::join(middleInputOriginIds.begin(), middleInputOriginIds.end(), ", "),
                       fmt::join(rightInputOriginIds.begin(), rightInputOriginIds.end(), ", "));
}

}// namespace NES
