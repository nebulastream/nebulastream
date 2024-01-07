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

#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <cstdint>
#include <string>

namespace NES::Nautilus::IR::Operations {

ConstIntOperation::ConstIntOperation(OperationIdentifier identifier, int64_t constantValue, Types::StampPtr stamp)
    : Operation(OperationType::ConstIntOp, identifier, stamp), constantValue(constantValue) {}

int64_t ConstIntOperation::getValue() { return constantValue; }
bool ConstIntOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::ConstIntOp; }

std::string ConstIntOperation::toString() { return identifier + " = " + std::to_string(constantValue); }

}// namespace NES::Nautilus::IR::Operations