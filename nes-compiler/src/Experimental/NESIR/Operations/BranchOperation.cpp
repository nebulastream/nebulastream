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

#include "Experimental/NESIR/BasicBlocks/BasicBlock.hpp"
#include <Experimental/NESIR/Operations/BranchOperation.hpp>

namespace NES {

BranchOperation::BranchOperation(BranchType type, BasicBlockPtr nextBlock)
    : Operation(OperationType::BranchOp), type(type), nextBlock(std::move(nextBlock)) {}

BranchOperation::BranchType BranchOperation::getType() { return type; }
BasicBlockPtr BranchOperation::getNextBlock() { return nextBlock; }

bool BranchOperation::classof(const NES::Operation* Op) { return Op->getOperationType() == OperationType::BranchOp; }

}// namespace NES
