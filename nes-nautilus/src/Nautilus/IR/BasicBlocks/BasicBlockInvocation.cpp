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
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>

namespace NES::Nautilus::IR::Operations {

BasicBlockInvocation::BasicBlockInvocation()
    : Operation(OperationType::BlockInvocation, Types::StampFactory::createVoidStamp()) {}

void BasicBlockInvocation::setBlock(BasicBlockPtr block) { this->basicBlock = block; }

BasicBlockPtr BasicBlockInvocation::getBlock() const { return basicBlock; }

void BasicBlockInvocation::addArgument(OperationPtr argument) {
    this->operations.emplace_back(argument);
    argument->addUsage(this);
}

void BasicBlockInvocation::removeArgument(uint64_t argumentIndex) { operations.erase(operations.begin() + argumentIndex); }

int BasicBlockInvocation::getOperationArgIndex(Operations::OperationPtr arg) {
    for (uint64_t i = 0; i < operations.size(); i++) {
        if (operations[i].lock() == arg) {
            return i;
        }
    }
    return -1;
}

std::vector<OperationPtr> BasicBlockInvocation::getArguments() const {
    std::vector<OperationPtr> arguments;
    for (auto& arg : this->operations) {
        arguments.emplace_back(arg.lock());
    }
    return arguments;
}
std::string BasicBlockInvocation::toString() { return "BasicBlockInvocation"; }

}// namespace NES::Nautilus::IR::Operations