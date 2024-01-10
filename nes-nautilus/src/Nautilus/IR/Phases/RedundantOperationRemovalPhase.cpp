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

#include "Nautilus/IR/BasicBlocks/BasicBlockArgument.hpp"
#include "Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp"
#include "Nautilus/IR/Operations/ConstBooleanOperation.hpp"
#include "Nautilus/IR/Operations/ConstFloatOperation.hpp"
#include "Nautilus/IR/Operations/ConstIntOperation.hpp"
#include "Nautilus/IR/Operations/LoadOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp"
#include "Nautilus/IR/Operations/ReturnOperation.hpp"
#include "Nautilus/IR/Operations/StoreOperation.hpp"
#include "Nautilus/Util/IRDumpHandler.hpp"
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Phases/RedundantOperationRemovalPhase.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <string>
#include <sys/types.h>
#include <unordered_map>

namespace NES::Nautilus::IR {

void RedundantOperationRemovalPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = RedundantOperationRemovalPhaseContext(std::move(ir));
    phaseContext.process();
};

void RedundantOperationRemovalPhase::RedundantOperationRemovalPhaseContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    auto firstBasicBlock = rootOperation->getFunctionBasicBlock();
    removeRedundantOperations(firstBasicBlock);
    NES_TRACE("{}", ir->toString());
}

std::string getSignature(const Operations::OperationPtr& operation) {
    // Create signature for operation.
    switch(operation->getOperationType()) {
        case Operations::Operation::OperationType::AddOp: {
            auto addOp = std::static_pointer_cast<Operations::AddOperation>(operation);
            return addOp->getLeftInput()->getIdentifier() + "-" + addOp->getRightInput()->getIdentifier() + "-+";
        }
        case Operations::Operation::OperationType::AndOp: {
            auto andOp = std::static_pointer_cast<Operations::AndOperation>(operation);
            return andOp->getLeftInput()->getIdentifier() + "-" + andOp->getRightInput()->getIdentifier() + "-&&";
        }
        case Operations::Operation::OperationType::CompareOp: {
            auto compareOp = std::static_pointer_cast<Operations::CompareOperation>(operation);
            return compareOp->getLeftInput()->getIdentifier() + "-" + compareOp->getRightInput()->getIdentifier() + "-" + compareOp->getComparatorAsString();
        }
        case Operations::Operation::OperationType::DivOp: {
            auto divOp = std::static_pointer_cast<Operations::DivOperation>(operation);
            return divOp->getLeftInput()->getIdentifier() + "-" + divOp->getRightInput()->getIdentifier() + "-/";
        }
        case Operations::Operation::OperationType::MulOp: {
            auto mulOp = std::static_pointer_cast<Operations::MulOperation>(operation);
            return mulOp->getLeftInput()->getIdentifier() + "-" + mulOp->getRightInput()->getIdentifier() + "-*";
        }
        case Operations::Operation::OperationType::NegateOp: {
            auto negateOp = std::static_pointer_cast<Operations::NegateOperation>(operation);
            return negateOp->getInput()->getIdentifier() + "-!";
        }
        case Operations::Operation::OperationType::OrOp: {
            auto orOp = std::static_pointer_cast<Operations::OrOperation>(operation);
            return orOp->getLeftInput()->getIdentifier() + "-" + orOp->getRightInput()->getIdentifier() + "-||";
        }
        case Operations::Operation::OperationType::SubOp: {
            auto subOp = std::static_pointer_cast<Operations::SubOperation>(operation);
            return subOp->getLeftInput()->getIdentifier() + "-" + subOp->getRightInput()->getIdentifier() + "--";
        }
        // Todo: Handle Load and Store?
        // Because of SSA, loading from the same address means performing the same instruction.
        case Operations::Operation::OperationType::LoadOp: {
            auto loadOp = std::static_pointer_cast<Operations::LoadOperation>(operation);
            return loadOp->getAddress()->getIdentifier() + "-[]";
        }
        // Because of SSA, storing the same value into the same address means performing the same instruction.
        case Operations::Operation::OperationType::StoreOp: {
            auto storeOp = std::static_pointer_cast<Operations::StoreOperation>(operation);
            return storeOp->getAddress()->getIdentifier() + "-" + storeOp->getValue()->getIdentifier() + "-[]";
        }
        default:
            // NES_DEBUG("We do not support getting inputs for the operation: {}", this->toString());
            return "";
    }
}

void RedundantOperationRemovalPhase::RedundantOperationRemovalPhaseContext::removeRedundantOperations(IR::BasicBlockPtr currentBlock) {
    // Algorithm:
    // - per block:
    //      - iterate over all operations
    //      - per operation:
    //          - create an operation signature (that can be used to look up whether there is an equivalent operation in a map)
    //          - look up signature using the map
    //              true: replace operation with operation from map
    //                  - remove operation, and replace all occurrences (including terminator ops)
    //              false: add signature and operation to map, continue

    // Signature:
    // - input operations concatenated + static_cast<uint8_t>(operationType)
    // Operations::Operation::OperationType
    std::stack<IR::BasicBlockPtr> newBlocks;
    std::unordered_set<std::string> visitedBlocks;
    std::unordered_map<std::string, Operations::OperationPtr> operationMap;
    
    // Iterate over all blocks/nodes of the query graph, until all blocks have been visited.
    newBlocks.emplace(currentBlock);
    do {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock = newBlocks.top();
        newBlocks.pop();
        // Todo: actual work here
        size_t operationIndex = 0;
        for(const auto& operation : currentBlock->getOperations()) {
            // Calculate signature
            auto signature = getSignature(operation);
            if(signature != "") {
                if(operationMap.contains(signature)) {
                    // Remove operation, and replace occurrences of operation in all later operations.
                    currentBlock->removeOperation(operation);
                    for(size_t i = operationIndex; i < currentBlock->getOperations().size()- 1; ++i) {
                        auto currentOp = currentBlock->getOperations().at(i);
                        currentOp->replaceInput(operation, operationMap.at(signature));
                    }
                    // Decrement operationIndex since we removed an operation.
                    operationIndex--;
                } else {
                    operationMap.emplace(std::pair{signature, operation});
                }
            } else if(operation == currentBlock->getTerminatorOp()) {
                // add nextBlocks
                switch (operation->getOperationType()) {
                    case Operations::Operation::OperationType::IfOp: {
                        auto ifOp = std::static_pointer_cast<Operations::IfOperation>(operation);
                        if(!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
                            newBlocks.emplace(ifOp->getTrueBlockInvocation().getBlock());
                        }
                        if(ifOp->hasFalseCase() && !visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
                            newBlocks.emplace(ifOp->getFalseBlockInvocation().getBlock());
                        }
                        break;
                    }
                    case Operations::Operation::OperationType::BranchOp: {
                        auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(operation);
                        if(!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
                            newBlocks.emplace(branchOp->getNextBlockInvocation().getBlock());
                        }
                        break;
                    }
                    case Operations::Operation::OperationType::ReturnOp: {
                        // auto returnOp = std::static_pointer_cast<Operations::ReturnOperation>(blockOp);
                        break;
                    }
                    default: {
                        NES_ERROR("A terminator operation was neither an ifOp, a branchOp or a returnUp");
                    }
                }
            }
            ++operationIndex;
        }

    } while (!newBlocks.empty());
}

}//namespace NES::Nautilus::IR