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

#include "Experimental/NESIR/Operations/Operation.hpp"
#include <Experimental/NESIR/Operations/ConstIntOperation.hpp>
#include <cstdint>
#include <string>

namespace NES::ExecutionEngine::Experimental::IR::Operations {

ConstIntOperation::ConstIntOperation(std::string identifier, int64_t constantValue, uint8_t numBits)
    : Operation(OperationType::ConstIntOp), identifier(std::move(identifier)), constantValue(constantValue), numBits(numBits){}

std::string ConstIntOperation::getIdentifier() { return identifier; }
int64_t ConstIntOperation::getConstantIntValue() { return constantValue; }
int8_t ConstIntOperation::getNumBits() { return numBits; }
bool ConstIntOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::ConstIntOp; }

std::string ConstIntOperation::toString() {
    return "ConstantInt" + std::to_string(numBits) + "Operation_" + identifier + "(" + std::to_string(constantValue) + ")";
}

template<class T>
T ConstIntOperation::getIntegerViaType() {
    switch(numBits) {
        case 8:
            return (int8_t) constantValue;
        case 16:
            return (int16_t) constantValue;
        case 32:
            return (int32_t) constantValue;
        case 64:
            return constantValue;
        default:
            return constantValue;
    }
}
}// namespace NES