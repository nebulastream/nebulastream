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
#include <Operators/AbstractOperators/Arity/SenaryOperator.hpp>
#include <Util/OperatorsUtil.hpp>
#include <fmt/format.h>

namespace NES {

SenaryOperator::SenaryOperator(OperatorId id) : Operator(id) {}


void SenaryOperator::setoneInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->oneInputSchema = std::move(inputSchema);
    }
}

void SenaryOperator::settwoInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->twoInputSchema = std::move(inputSchema);
    }
}

void SenaryOperator::setthreeInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->threeInputSchema = std::move(inputSchema);
    }
}

void SenaryOperator::setfourInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->fourInputSchema = std::move(inputSchema);
    }
}

void SenaryOperator::setfiveInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->fiveInputSchema = std::move(inputSchema);
    }
}

void SenaryOperator::setsixInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->sixInputSchema = std::move(inputSchema);
    }
}

void SenaryOperator::setOutputSchema(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}

SchemaPtr SenaryOperator::getOutputSchema() const {
    return outputSchema;
}

std::vector<OriginId> SenaryOperator::getoneInputOriginIds() {
    return oneInputOriginIds;
}

std::vector<OriginId> SenaryOperator::gettwoInputOriginIds() {
    return twoInputOriginIds;
}

std::vector<OriginId> SenaryOperator::getthreeInputOriginIds() {
    return threeInputOriginIds;
}

std::vector<OriginId> SenaryOperator::getfourInputOriginIds() {
    return fourInputOriginIds;
}

std::vector<OriginId> SenaryOperator::getfiveInputOriginIds() {
    return fiveInputOriginIds;
}

std::vector<OriginId> SenaryOperator::getsixInputOriginIds() {
    return sixInputOriginIds;
}

std::vector<OriginId> SenaryOperator::getOutputOriginIds() const {
    return const_cast<SenaryOperator*>(this)->getAllInputOriginIds();
}

std::vector<OriginId> SenaryOperator::getAllInputOriginIds() {
    std::vector<OriginId> allOriginIds;
    allOriginIds.insert(allOriginIds.end(), oneInputOriginIds.begin(), oneInputOriginIds.end());
    allOriginIds.insert(allOriginIds.end(), twoInputOriginIds.begin(), twoInputOriginIds.end());
    allOriginIds.insert(allOriginIds.end(), threeInputOriginIds.begin(), threeInputOriginIds.end());
    allOriginIds.insert(allOriginIds.end(), fourInputOriginIds.begin(), fourInputOriginIds.end());
    allOriginIds.insert(allOriginIds.end(), fiveInputOriginIds.begin(), fiveInputOriginIds.end());
    allOriginIds.insert(allOriginIds.end(), sixInputOriginIds.begin(), sixInputOriginIds.end());
    return allOriginIds;
}


std::string SenaryOperator::toString() const {
    return fmt::format("oneInputSchema: {}\n"
                      "twoInputSchema: {}\n"
                      "threeInputSchema: {}\n"
                      "fourInputSchema: {}\n"
                      "fiveInputSchema: {}\n"
                      "sixInputSchema: {}\n"
                      "outputSchema: {}\n"
                      "distinctSchemas: {}\n"
                      "oneInputOriginIds: {}\n"
                      "twoInputOriginIds: {}\n"
                      "threeInputOriginIds: {}\n"
                      "fourInputOriginIds: {}\n"
                      "fiveInputOriginIds: {}\n"
                      "sixInputOriginIds: {}",
                      oneInputSchema->toString(),
                      twoInputSchema->toString(),
                      threeInputSchema->toString(),
                      fourInputSchema->toString(),
                      fiveInputSchema->toString(),
                      sixInputSchema->toString(),
                      outputSchema->toString(),
                      Util::concatenateVectorAsString(distinctSchemas),
                      fmt::join(oneInputOriginIds.begin(), oneInputOriginIds.end(), ", "),
                      fmt::join(twoInputOriginIds.begin(), twoInputOriginIds.end(), ", "),
                      fmt::join(threeInputOriginIds.begin(), threeInputOriginIds.end(), ", "),
                      fmt::join(fourInputOriginIds.begin(), fourInputOriginIds.end(), ", "),
                      fmt::join(fiveInputOriginIds.begin(), fiveInputOriginIds.end(), ", "),
                      fmt::join(sixInputOriginIds.begin(), sixInputOriginIds.end(), ", "));
}

} // namespace NES