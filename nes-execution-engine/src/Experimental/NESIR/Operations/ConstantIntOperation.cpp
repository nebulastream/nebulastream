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
#include <Experimental/NESIR/Operations/ConstantIntOperation.hpp>
#include <cstdint>

namespace NES {

ConstantIntOperation::ConstantIntOperation(std::string identifier, int64_t constantValue, int8_t numBits)
    : Operation(OperationType::ConstantOp), identifier(std::move(identifier)), constantValue(constantValue), numBits(numBits){}

std::string ConstantIntOperation::getIdentifier() { return identifier; }
int64_t ConstantIntOperation::getConstantIntValue() { return constantValue; }
int8_t ConstantIntOperation::getNumBits() { return numBits; }
bool ConstantIntOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::ConstantOp; }

template<class T>
T ConstantIntOperation::getIntegerViaType() {
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