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

#include "Nautilus/IR/BasicBlocks/BasicBlock.hpp"
#include <Experimental/MLIR/YieldOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
namespace NES::Nautilus::IR::Operations {

YieldOperation::YieldOperation(std::vector<OperationPtr> arguments, BasicBlockPtr nextBlock)
    : Operation(OperationType::MLIR_YIELD, Types::StampFactory::createVoidStamp()), nextBlock(nextBlock) {
    for (auto& arg : arguments) {
        this->arguments.emplace_back(arg);
    }
}

std::vector<OperationPtr> YieldOperation::getArguments() {
    std::vector<OperationPtr> arguments;
    for (auto& arg : this->arguments) {
        arguments.emplace_back(arg.lock());
    }
    return arguments;
}

std::string YieldOperation::toString() {
    std::string baseString = "yield (";
    if (getArguments().size() > 0) {
        baseString += getArguments().at(0)->getIdentifier();
        for (int i = 1; i < (int) getArguments().size(); ++i) {
            baseString += ", " + getArguments().at(i)->getIdentifier();
        }
    }
    return baseString + ")";
}
bool YieldOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::MLIR_YIELD; }

}// namespace NES::Nautilus::IR::Operations
