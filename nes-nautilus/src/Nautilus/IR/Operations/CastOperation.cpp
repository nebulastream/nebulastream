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

CastOperation::CastOperation(OperationIdentifier identifier, Operation& input, Types::StampPtr targetStamp)
    : Operation(OperationType::CastOp, identifier, targetStamp), input(input) {
    input.addUsage(*this);
}

const Operation& CastOperation::getInput() const { return input; }

std::string CastOperation::toString() const {
    return fmt::format("{} = {} cast_to {}", identifier, getInput().getIdentifier(), getStamp()->toString());
}

}// namespace NES::Nautilus::IR::Operations