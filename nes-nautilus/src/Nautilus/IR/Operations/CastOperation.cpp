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
#include <Nautilus/IR/Operations/CastOperation.hpp>

namespace NES::Nautilus::IR::Operations {

CastOperation::CastOperation(OperationIdentifier identifier, OperationPtr input, Types::StampPtr targetStamp)
    : Operation(OperationType::CastOp, identifier, targetStamp), input(input) {
    input->addUsage(this);
}

OperationPtr CastOperation::getInput() { return input.lock(); }

std::string CastOperation::toString() {
    return identifier + " = " + getInput()->getIdentifier() + " cast_to " + getStamp()->toString();
}

}// namespace NES::Nautilus::IR::Operations