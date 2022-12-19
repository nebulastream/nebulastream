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

BasicBlockPtr BasicBlockInvocation::getNextBlock() { return basicBlock; }

void BasicBlockInvocation::addArgument(OperationPtr argument) {
    this->branchOps.emplace_back(argument);
    argument->addUsage(this);
}

void BasicBlockInvocation::setBranchOpAt(size_t index, Operations::OperationPtr operation) { 
    this->branchOps.at(index) = operation; 
}
void BasicBlockInvocation::clearBranchOps() { this->branchOps.clear(); }

//Todo remove
void BasicBlockInvocation::removeArgument(uint64_t argumentIndex) { branchOps.erase(branchOps.begin() + argumentIndex); }

int BasicBlockInvocation::getOperationArgIndex(Operations::OperationPtr arg) {
    for (uint64_t i = 0; i < branchOps.size(); i++) {
        if (branchOps[i] == arg) {
        // if (operations[i].lock() == arg) {
            return i;
        }
    }
    return -1;
}

std::vector<OperationPtr> BasicBlockInvocation::getBranchOps() {
    // std::vector<OperationPtr> arguments;
    // for (auto& arg : this->operations) {
    //     arguments.emplace_back(arg.lock());
    // }
    // return arguments;
    // changed because StructuredControlFlowPhaseTest 9 with value scoping lead to weak pointer returning null pointer 
    // even though the underlying pointer of the shared pointer was still alive
    return this->branchOps;
}
std::string BasicBlockInvocation::toString() { return "BasicBlockInvocation"; }

}// namespace NES::Nautilus::IR::Operations