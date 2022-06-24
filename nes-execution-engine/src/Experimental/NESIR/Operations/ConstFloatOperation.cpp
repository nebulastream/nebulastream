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
#include <Experimental/NESIR/Operations/ConstFloatOperation.hpp>
#include <cstdint>
#include <string>

namespace NES::ExecutionEngine::Experimental::IR::Operations {

ConstFloatOperation::ConstFloatOperation(OperationIdentifier identifier,  double constantValue, uint8_t numBits)
    : Operation(OperationType::ConstFloatOp, identifier, FLOAT), constantValue(constantValue), numBits(numBits){}

double ConstFloatOperation::getConstantFloatValue() { return constantValue; }
int8_t ConstFloatOperation::getNumBits() { return numBits; }
bool ConstFloatOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::ConstIntOp; }

std::string ConstFloatOperation::toString() {
    return "ConstantFloat" + std::to_string(numBits) + "Operation_" + identifier + "(" + std::to_string(constantValue) + ")";
}

template<class T>
T ConstFloatOperation::getFloatViaType() {
    switch(numBits) {
        case 32:
            return (float) constantValue;
        case 64:
            return constantValue;
        default:
            return constantValue;
    }
}
}// namespace NES