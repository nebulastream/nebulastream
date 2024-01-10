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

#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <cstdint>
#include <string>
namespace NES::Nautilus::IR::Operations {
ReturnOperation::ReturnOperation() : Operation(Operation::OperationType::ReturnOp, Types::StampFactory::createVoidStamp()) {}
ReturnOperation::ReturnOperation(OperationPtr returnValue)
    : Operation(Operation::OperationType::ReturnOp, returnValue->getStamp()), returnValue(returnValue) {
    returnValue->addUsage(this);
}

std::string ReturnOperation::toString() {
    if (hasReturnValue()) {
        return "return (" + getReturnValue()->getIdentifier() + ")";
    } else {
        return "return";
    }
}
OperationPtr ReturnOperation::getReturnValue() { return returnValue.lock(); }
void ReturnOperation::setReturnValue(OperationWPtr newReturnValue) { 
    this->returnValue = newReturnValue;    
}
bool ReturnOperation::hasReturnValue() { return !stamp->isVoid(); }

}// namespace NES::Nautilus::IR::Operations
