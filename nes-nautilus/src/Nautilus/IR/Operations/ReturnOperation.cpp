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

#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <string>

namespace NES::Nautilus::IR::Operations {
ReturnOperation::ReturnOperation() : Operation(Operation::OperationType::ReturnOp, Types::StampFactory::createVoidStamp()) {}
ReturnOperation::ReturnOperation(Operation& returnValue)
    : Operation(Operation::OperationType::ReturnOp, returnValue.getStamp()), returnValue(&returnValue) {
    returnValue.addUsage(*this);
}

std::string ReturnOperation::toString() const {
    if (hasReturnValue()) {
        return "return (" + getReturnValue().getIdentifier() + ")";
    } else {
        return "return";
    }
}
const Operation& ReturnOperation::getReturnValue() const {
    NES_ASSERT(returnValue != nullptr, "Unchecked call for ReturnOperation::getReturnValue()");
    return *returnValue;
}
bool ReturnOperation::hasReturnValue() const { return !stamp->isVoid(); }

}// namespace NES::Nautilus::IR::Operations
