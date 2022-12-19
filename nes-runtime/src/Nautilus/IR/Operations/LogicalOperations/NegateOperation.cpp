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

#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
namespace NES::Nautilus::IR::Operations {

NegateOperation::NegateOperation(OperationIdentifier identifier, OperationPtr input)
    : Operation(OperationType::NegateOp, identifier, Types::StampFactory::createBooleanStamp()), negateInput(std::move(input)) {
    input->addUsage(this);
}

std::string NegateOperation::toString() { return identifier + "= not " + getNegateInput()->getIdentifier(); }
bool NegateOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::AddOp; }
OperationPtr NegateOperation::getNegateInput() { return negateInput.lock(); }
void NegateOperation::setNegateInput(OperationPtr input) { this->negateInput = input; }
}// namespace NES::Nautilus::IR::Operations