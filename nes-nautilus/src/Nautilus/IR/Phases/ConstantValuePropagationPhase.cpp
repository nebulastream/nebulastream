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
#include "Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp"
#include "Nautilus/IR/Operations/ReturnOperation.hpp"
#include "Nautilus/Util/IRDumpHandler.hpp"
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Phases/ConstantValuePropagationPhase.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <string>
#include <unordered_map>

namespace NES::Nautilus::IR {

void ConstantValuePropagationPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = ConstantValuePropagationPhaseContext(std::move(ir));
    phaseContext.process();
};

void ConstantValuePropagationPhase::ConstantValuePropagationPhaseContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    auto firstBasicBlock = rootOperation->getFunctionBasicBlock();
    static constexpr uint32_t NUM_ITERATIONS = 2;
    for(uint32_t i = 0; i < NUM_ITERATIONS; ++i) {
        buildConstOpMap(firstBasicBlock);
        propagateConstantValues();
        foldConstantValues(firstBasicBlock);
    }
    NES_TRACE("{}", ir->toString());
}

void ConstantValuePropagationPhase::ConstantValuePropagationPhaseContext::handleBlockInvocation(
    Operations::BasicBlockInvocation basicBlockInvocation)
{
    auto argIndex = 0;
    for(const auto& brOpArg : basicBlockInvocation.getArguments()) {
        auto nextBlockArgIdentifier = basicBlockInvocation.getBlock()->getArguments().at(argIndex)->getIdentifier();
        if(constOpMap.contains(brOpArg->getIdentifier())) {
            // map index of br op to index of nextBlock
            // Todo: what if the op is a BasicBlockArgument?
            // auto constOpArg = std::static_pointer_cast<Operations::ConstIntOperation>(brOpArg);
            // Place constOpArg into constOpMap. Transfer rootOp..
            auto rootConstOp = constOpMap.at(brOpArg->getIdentifier()).rootConstOp;
            auto constValue = constOpMap.at(rootConstOp->getIdentifier()).constValue;
            constOpMap.emplace(std::pair{
                nextBlockArgIdentifier,
                ConstOpInfo{basicBlockInvocation.getBlock()->getArguments().at(argIndex), 
                    basicBlockInvocation.getBlock(), rootConstOp, constValue, false, false}});
        } else {
            nonConstOpArgs.emplace(nextBlockArgIdentifier);
        }
        ++argIndex;
    }
}


void ConstantValuePropagationPhase::ConstantValuePropagationPhaseContext::detectComplexVariable(
    Operations::BasicBlockInvocation basicBlockInvocation)
{
    auto argIndex = 0;
    for(const auto& brOpArg : basicBlockInvocation.getArguments()) {
        auto nextBlockArgIdentifier = basicBlockInvocation.getBlock()->getArguments().at(argIndex)->getIdentifier();
        if(constOpMap.contains(nextBlockArgIdentifier)) {
            // If reachableFrom of nextBlockArgument is different to constOpArg, mark as loop
            // If the brOpArg is not in the constOpMap, it is a modified value and therefore leads to a loop.
            if(!constOpMap.contains(brOpArg->getIdentifier()) || 
                constOpMap.at(brOpArg->getIdentifier()).rootConstOp->getIdentifier() 
                != constOpMap.at(nextBlockArgIdentifier).rootConstOp->getIdentifier())
            {
                constOpMap.at(constOpMap.at(nextBlockArgIdentifier).rootConstOp->getIdentifier()).isComplex = true;
            }
        } else {
            // if nextBlockArg is a non-constOp, but the current brOpArg is a constOp, mark brOpArg as complex.
            // The nextBlockArg is a merge arg and a constValue might have been changed along the different paths.
            if(nonConstOpArgs.contains(nextBlockArgIdentifier) && constOpMap.contains(brOpArg->getIdentifier())) {
                constOpMap.at(brOpArg->getIdentifier()).isComplex = true;
                constOpMap.at(constOpMap.at(brOpArg->getIdentifier()).rootConstOp->getIdentifier()).isComplex = true;
            }
        }
        ++argIndex;
    }
}


void ConstantValuePropagationPhase::ConstantValuePropagationPhaseContext::processBlock (
    IR::BasicBlockPtr currentBlock,
    std::stack<IR::BasicBlockPtr>& newBlocks,
    std::unordered_set<std::string> visitedBlocks) {

    // If we iterate BFS, we always know that the BBA will be later BBAs
    for(const auto& blockOp : currentBlock->getOperations()) {
        // Check if terminator operation
        switch(blockOp->getOperationType()) {
            case Operations::Operation::OperationType::BranchOp: {
                auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(blockOp);
                // Add next block, if not visited yet.
                if(!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {                
                    // Iterate over arguments.
                    handleBlockInvocation(branchOp->getNextBlockInvocation());
                    newBlocks.emplace(branchOp->getNextBlockInvocation().getBlock());
                    // Todo: also check merge-block args
                } else {
                    detectComplexVariable(branchOp->getNextBlockInvocation());
                }
                if(branchOp->getNextBlockInvocation().getBlock()->getPredecessors().size() > 1) {
                    detectComplexVariable(branchOp->getNextBlockInvocation());
                }
                break;
            }
            case Operations::Operation::OperationType::IfOp: {
                auto ifOp = std::static_pointer_cast<Operations::IfOperation>(blockOp);
                if(constOpMap.contains(ifOp->getBooleanValue()->getIdentifier())) {
                    constOpMap.at(ifOp->getBooleanValue()->getIdentifier()).isUsed = true;
                }
                // Add next block, if not visited yet.
                if(!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) { 
                    handleBlockInvocation(ifOp->getTrueBlockInvocation());
                    newBlocks.emplace(ifOp->getTrueBlockInvocation().getBlock());
                    if(ifOp->getTrueBlockInvocation().getBlock()->getPredecessors().size() > 1) {
                        detectComplexVariable(ifOp->getTrueBlockInvocation());
                    }
                } else {
                    detectComplexVariable(ifOp->getTrueBlockInvocation());
                }
                if(ifOp->hasFalseCase()) {
                    // Add next block, if not visited yet.
                    if(!visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) { 
                        handleBlockInvocation(ifOp->getFalseBlockInvocation());
                        newBlocks.emplace(ifOp->getFalseBlockInvocation().getBlock());
                        if(ifOp->getFalseBlockInvocation().getBlock()->getPredecessors().size() > 1) {
                        detectComplexVariable(ifOp->getFalseBlockInvocation());
                    }
                    } else {
                        detectComplexVariable(ifOp->getFalseBlockInvocation());
                    }
                }
                break;
            }
            case Operations::Operation::OperationType::ReturnOp: { 
                NES_DEBUG("Reached Return Operation.");
                for(const auto& opArg : blockOp->getInputs()) {     
                    if(constOpMap.contains(opArg->getIdentifier())) {
                        // ConstOp is used by another operation.
                        constOpMap.at(opArg->getIdentifier()).isUsed = true;
                    }
                }
                break;
            }
            // Else, check if constOp
            default: {
                if(blockOp->isConstOperation()) {
                    // If is const operation, add operation to map.
                    if(blockOp->getOperationType() == Operations::Operation::OperationType::ConstIntOp) {
                        auto constIntOp = std::static_pointer_cast<Operations::ConstIntOperation>(blockOp);
                        auto constValue = constIntOp->getValue();
                        // A value is always reachable from itself.
                        constOpMap.emplace(std::pair{blockOp->getIdentifier(), ConstOpInfo{constIntOp, currentBlock, constIntOp, constValue, false, false}});
                    }
                    // Todo: also cover other types -> can use std::variant
                } else {
                    // If not terminator nor const op, check arguments.
                    for(const auto& opArg : blockOp->getInputs()) {
                        // The 1_1 weak pointers to 1_0 at some point turn to nullptr ()
                        if(constOpMap.contains(opArg->getIdentifier())) {
                            // ConstOp is used by another operation.
                            // constOpMap.at(constOpMap.at(opArg->getIdentifier()).reachableFrom->getIdentifier()).isUsed = true;
                            constOpMap.at(opArg->getIdentifier()).isUsed = true;
                        }
                    }
                }
            }
        }
    }
    visitedBlocks.emplace(currentBlock->getIdentifier());
}

void ConstantValuePropagationPhase::ConstantValuePropagationPhaseContext::buildConstOpMap(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> newBlocks;
    std::unordered_set<std::string> visitedBlocks;
    // Reset class member variables.
    this->constOpMap = std::unordered_map<std::string, ConstOpInfo>();
    this->nonConstOpArgs = std::unordered_set<std::string>();

    // Iterate over all blocks/nodes of the query graph, until all blocks have been visited.
    newBlocks.emplace(currentBlock);
    do {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock = newBlocks.top();
        newBlocks.pop();
        processBlock(currentBlock, newBlocks, visitedBlocks);
    } while (!newBlocks.empty());
}

void removeOperationFromNextBlockInvocations(const Operations::OperationPtr& terminatorOp, const Operations::OperationPtr& opToRemove) {
    if(terminatorOp->getOperationType() == Operations::Operation::OperationType::BranchOp) {
        auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(terminatorOp);
        auto argIndex = branchOp->getNextBlockInvocation().getOperationArgIndex(opToRemove);
        // Todo: does removeArgument also work with -1?
        if(argIndex != -1) {
            branchOp->getNextBlockInvocation().removeArgument(argIndex);
        }
    } else if(terminatorOp->getOperationType() == Operations::Operation::OperationType::IfOp) {
        auto ifOp = std::static_pointer_cast<Operations::IfOperation>(terminatorOp);

        auto argIndexTrueBlock = ifOp->getTrueBlockInvocation().getOperationArgIndex(opToRemove);
        if(argIndexTrueBlock != -1) {
            ifOp->getTrueBlockInvocation().removeArgument(argIndexTrueBlock);
        }
        auto argIndexFalseBlock = ifOp->getFalseBlockInvocation().getOperationArgIndex(opToRemove);
        if(argIndexFalseBlock != -1) {
            ifOp->getFalseBlockInvocation().removeArgument(argIndexFalseBlock);
        }
    }
}

void ConstantValuePropagationPhase::ConstantValuePropagationPhaseContext::removeConstOpFromBlock(
    const Operations::OperationPtr& constOp, const NES::Nautilus::IR::BasicBlockPtr& currentBlock) 
{
    // 1. Remove from arguments and operations.
    if(constOp->getOperationType() == Operations::Operation::OperationType::BasicBlockArgument) {
        auto basicBlockArgument = std::static_pointer_cast<Operations::BasicBlockArgument>(constOp);
        currentBlock->removeArgument(basicBlockArgument);
    } else {
        currentBlock->removeOperation(constOp);
    }

    // 3. Remove from nextBlockInvocations.
    removeOperationFromNextBlockInvocations(currentBlock->getTerminatorOp(), constOp);
}

void ConstantValuePropagationPhase::ConstantValuePropagationPhaseContext::replaceInputArgumentUsages(
    Operations::OperationPtr toReplace, Operations::OperationPtr replaceWith, BasicBlockPtr currentBlock) 
{   
    // Todo: why not replace usages in the terminator ops?
    for(auto blockOp : currentBlock->getOperations()) {
        // if(blockOp != currentBlock->getTerminatorOp()) {
            blockOp->replaceInput(toReplace, replaceWith);
        // }
    }
}

void ConstantValuePropagationPhase::ConstantValuePropagationPhaseContext::propagateConstantValues() {
    // Create const value map for current block
    std::unordered_map<std::string, Operations::OperationPtr> constOpsInBasicBlock;
    

    for(const auto& [opName, constOpInfo] : constOpMap) {
        // If op is not a loop variable.
        if(!constOpMap.at(constOpInfo.rootConstOp->getIdentifier()).isComplex) {
        // if(!constOpInfo.isLoop) {
            if(!constOpInfo.isUsed) {
                // ConstOp is not used by any block. Simply remove.
                removeConstOpFromBlock(constOpInfo.operation, constOpInfo.definedInBlock);
            } else {
                // ConstOp is used used in its block.
                // If BasicBlockArgument, remove from arguments, and nextBlockInvocations.
                auto blockValueString = constOpInfo.definedInBlock->getIdentifier() + std::to_string(constOpInfo.rootConstOp->getValue());
                if(constOpInfo.operation->getOperationType() == Operations::Operation::OperationType::BasicBlockArgument) {
                    auto basicBlockArgument = std::static_pointer_cast<Operations::BasicBlockArgument>(constOpInfo.operation);
                    removeConstOpFromBlock(constOpInfo.operation, constOpInfo.definedInBlock);
                    // If there is a constOp with the exact same value in the current BB already, replace usages.
                    if(constOpsInBasicBlock.contains(blockValueString)) {
                        replaceInputArgumentUsages(constOpInfo.operation, constOpsInBasicBlock.at(blockValueString), constOpInfo.definedInBlock);
                    } else {
                        // The value was never used before. Place new constIntOp at the top of operations. Use arg name.
                        auto newConstIntOp = std::make_shared<Operations::ConstIntOperation>(opName, constOpInfo.constValue, constOpInfo.rootConstOp->getStamp());
                        // Todo 17-1: When replacing a basicBlockArgument, we need to make sure that its usages are also replaced
                        // in the input operations of the basic block.
                        // Todo: 
                        // - compare operation of IFValue is NOT changed (in ConstFolding phase)!
                        // -> value is int instead of boolean
                        constOpInfo.definedInBlock->addOperationBefore(constOpInfo.definedInBlock->getOperationAt(0), newConstIntOp);
                        constOpsInBasicBlock.emplace(std::pair{blockValueString, newConstIntOp});
                        replaceInputArgumentUsages(constOpInfo.operation, newConstIntOp, constOpInfo.definedInBlock);
                    }
                } else if(constOpInfo.operation->getOperationType() == Operations::Operation::OperationType::ConstIntOp) {
                    // The current operation is a not an argument. It is a root constIntOp.
                    // If it is not covered by another constIntOp already, delete it from the current block and add at the beginning.
                    auto constIntOp = std::static_pointer_cast<Operations::ConstIntOperation>(constOpInfo.operation);
                    if(constOpsInBasicBlock.contains(blockValueString)) {
                        // Todo: The problem: the constIntOp is used, there is another constIntOp with the same value, the ifOp boolVal is not updated
                        replaceInputArgumentUsages(constOpInfo.operation, constOpsInBasicBlock.at(blockValueString), constOpInfo.definedInBlock);
                        removeConstOpFromBlock(constOpInfo.operation, constOpInfo.definedInBlock);
                    } else {
                        removeConstOpFromBlock(constIntOp, constOpInfo.definedInBlock);
                        constOpInfo.definedInBlock->addOperationBefore(constOpInfo.definedInBlock->getOperationAt(0), constIntOp);
                        constOpsInBasicBlock.emplace(std::pair{blockValueString, constIntOp});
                    }
                } else {
                    NES_TRACE("ERROR: Operation in constOpMap is neither argument, nor constIntOp");
                }
            }
        }
    }
}

// template <typename T>
// Operations::OperationPtr createNewConstOp(Operations::OperationPtr blockOp, std::function<int(int, int)> function) {
//     assert(blockOp->getInputs().size() == 2);
//     auto binaryOperation = std::static_pointer_cast<T>(blockOp);
//     auto firstValue = std::static_pointer_cast<Operations::ConstIntOperation>(binaryOperation->getLeftInput())->getValue();
//     auto secondValue = std::static_pointer_cast<Operations::ConstIntOperation>(binaryOperation->getRightInput())->getValue();
//     return std::make_shared<Operations::ConstIntOperation>(
//         blockOp->getIdentifier(), 
//         function(firstValue, secondValue), blockOp->getStamp());
// }

Operations::OperationPtr createNewConstOpFromOldOp(Operations::OperationPtr blockOp) {
    switch(blockOp->getOperationType()) {
        case Operations::Operation::OperationType::AddOp : {
            auto addOp = std::static_pointer_cast<Operations::AddOperation>(blockOp);
            auto firstValue = std::static_pointer_cast<Operations::ConstIntOperation>(addOp->getLeftInput())->getValue();
            auto secondValue = std::static_pointer_cast<Operations::ConstIntOperation>(addOp->getRightInput())->getValue();
            return std::make_shared<Operations::ConstIntOperation>(
                blockOp->getIdentifier(), 
                firstValue + secondValue, blockOp->getStamp());
            // return createNewConstOp<Operations::AddOperation>(blockOp, [](int leftInput, int rightInput) -> int { return leftInput + rightInput; });
        }
        case Operations::Operation::OperationType::SubOp : {
            auto subOp = std::static_pointer_cast<Operations::SubOperation>(blockOp);
            auto firstValue = std::static_pointer_cast<Operations::ConstIntOperation>(subOp->getLeftInput())->getValue();
            auto secondValue = std::static_pointer_cast<Operations::ConstIntOperation>(subOp->getRightInput())->getValue();
            return std::make_shared<Operations::ConstIntOperation>(
                blockOp->getIdentifier(), 
                firstValue - secondValue, blockOp->getStamp());
            break;
        }
        case Operations::Operation::OperationType::MulOp : {
            auto mulOp = std::static_pointer_cast<Operations::MulOperation>(blockOp);
            auto firstValue = std::static_pointer_cast<Operations::ConstIntOperation>(mulOp->getLeftInput())->getValue();
            auto secondValue = std::static_pointer_cast<Operations::ConstIntOperation>(mulOp->getRightInput())->getValue();
            return std::make_shared<Operations::ConstIntOperation>(
                blockOp->getIdentifier(), 
                firstValue - secondValue, blockOp->getStamp());
            // return createNewConstOp<Operations::MulOperation>(blockOp);
            break;
        }
        case Operations::Operation::OperationType::DivOp : {
            auto divOp = std::static_pointer_cast<Operations::DivOperation>(blockOp);
            auto firstValue = std::static_pointer_cast<Operations::ConstIntOperation>(divOp->getLeftInput())->getValue();
            auto secondValue = std::static_pointer_cast<Operations::ConstIntOperation>(divOp->getRightInput())->getValue();
            return std::make_shared<Operations::ConstIntOperation>(
                blockOp->getIdentifier(), 
                firstValue - secondValue, blockOp->getStamp());
            // return createNewConstOp<Operations::DivOperation>(blockOp);
            break;
        }
        case Operations::Operation::OperationType::CompareOp : {
            auto compareOp = std::static_pointer_cast<Operations::CompareOperation>(blockOp);
            auto firstValue = std::static_pointer_cast<Operations::ConstIntOperation>(compareOp->getLeftInput())->getValue();
            auto secondValue = std::static_pointer_cast<Operations::ConstIntOperation>(compareOp->getRightInput())->getValue();
            bool result = false;
            switch(compareOp->getComparator()) {
                case Operations::CompareOperation::EQ : {
                    result = firstValue == secondValue;
                    break;
                }
                case Operations::CompareOperation::NE : {
                    result = firstValue != secondValue;
                    break;
                }
                case Operations::CompareOperation::LT : {
                    result = firstValue < secondValue;
                    break;
                }
                case Operations::CompareOperation::LE : {
                    result = firstValue <= secondValue;
                    break;
                }
                case Operations::CompareOperation::GT : {
                    result = firstValue > secondValue;
                    break;
                }
                case Operations::CompareOperation::GE : {
                    result = firstValue >= secondValue;
                    break;
                }
                default: {
                    NES_ERROR("Comparator type not supported.");
                    break;
                }
            }
            return std::make_shared<Operations::ConstBooleanOperation>(
                blockOp->getIdentifier(), 
                result);
        }
        case Operations::Operation::OperationType::AndOp : {
            auto andOp = std::static_pointer_cast<Operations::AndOperation>(blockOp);
            auto firstValue = std::static_pointer_cast<Operations::ConstIntOperation>(andOp->getLeftInput())->getValue();
            auto secondValue = std::static_pointer_cast<Operations::ConstIntOperation>(andOp->getRightInput())->getValue();
            return std::make_shared<Operations::ConstIntOperation>(
                blockOp->getIdentifier(), 
                firstValue && secondValue, blockOp->getStamp());
        }
        case Operations::Operation::OperationType::OrOp : {
            auto orOp = std::static_pointer_cast<Operations::OrOperation>(blockOp);
            auto firstValue = std::static_pointer_cast<Operations::ConstIntOperation>(orOp->getLeftInput())->getValue();
            auto secondValue = std::static_pointer_cast<Operations::ConstIntOperation>(orOp->getRightInput())->getValue();
            return std::make_shared<Operations::ConstIntOperation>(
                blockOp->getIdentifier(), 
                firstValue || secondValue, blockOp->getStamp());
        }
        default: {
            NES_DEBUG("Operation type not supported.");
            return nullptr;
        }
    }
}

void ConstantValuePropagationPhase::ConstantValuePropagationPhaseContext::foldConstantValues(IR::BasicBlockPtr currentBlock) {
    // Algorithm:
    // - iterate over all basic blocks
    // - for each basic block:
    //   - remember all const(Int)Ops
    //   - check if non-const(Int)Ops consist of constIntOps only
    //      - true: calculate the resulting const value, convert op to constIntOp, and assign new value
    //      - false: proceed
    // Todo: can loop args be problematic if we stick to a single basic block?
    // Todo: how to handle values that become useless?
    // 0_0 = 1 : int64
    // 0_1 = 2 : int64
    // 0_3 = 0_1 + 0_2 : int64
    // -> results in:
    // 0_0 = 1 : int64
    // 0_1 = 2 : int64
    // 0_3 = 3 : int64
    // -> if we apply the const prop phase again, 0_0 and 0_1 will be removed, since they are not used anymore
    //  -> how to handle removing ops?
    //  -> constant propagation phase? -> No, because the values are not the same
    //      -> yes, because if values were only used by the previous op, they will now not be used by that op anymore and therefore will be removed
    std::stack<IR::BasicBlockPtr> newBlocks;
    std::unordered_set<std::string> visitedBlocks;
    std::unordered_map<std::string, Operations::OperationPtr> replacedOperations;
    
    // Iterate over all blocks/nodes of the query graph, until all blocks have been visited.
    newBlocks.emplace(currentBlock);
    do {
        visitedBlocks.emplace(currentBlock->getIdentifier());
        currentBlock = newBlocks.top();
        newBlocks.pop();
        // Todo: actual work here
        auto blockOpIndex = 0;
        for(auto const& blockOp : currentBlock->getOperations()) {
            bool constInputOpsOnly = true;
            // Todo: in principle, we also need to adapt args.
            if(!blockOp->isConstOperation()) {
                for(auto const& inputOp : blockOp->getInputs()) {
                    bool inputOpWasReplacedWithConst = replacedOperations.contains(inputOp->getIdentifier());
                    if(inputOpWasReplacedWithConst) {
                        blockOp->replaceInput(inputOp, replacedOperations.at(inputOp->getIdentifier()));
                    }
                    // Todo: only considering ConstIntOp
                    constInputOpsOnly &= inputOpWasReplacedWithConst || (inputOp->getOperationType() == Operations::Operation::OperationType::ConstIntOp);
                }
            }
            if(blockOp != currentBlock->getTerminatorOp() && !blockOp->isConstOperation()) {
                // For each operation, check whether it consists of constOps only
                // for(auto const& inputOp : blockOp->getInputs()) {
                //     constInputOpsOnly &= inputOp->getOperationType() == Operations::Operation::OperationType::ConstIntOp;
                // }
                // If all operations are const ops (currently we only support constIntOps), calculate the resulting const.
                if(constInputOpsOnly) {
                    // Calculate the resulting const.
                    Operations::OperationPtr newConstIntOp = createNewConstOpFromOldOp(blockOp);

                    // Remove old operation and insert new operation (update)
                    if(newConstIntOp) {
                        currentBlock->replaceOperation(blockOpIndex, newConstIntOp);
                        replacedOperations.emplace(std::pair{newConstIntOp->getIdentifier(), newConstIntOp});
                    }
                }
            } else {
                if(blockOp->getOperationType() == Operations::Operation::OperationType::BranchOp) {
                    auto branchOp = static_pointer_cast<Operations::BranchOperation>(blockOp);
                    if(!visitedBlocks.contains(branchOp->getNextBlockInvocation().getBlock()->getIdentifier())) {
                        newBlocks.emplace(branchOp->getNextBlockInvocation().getBlock());
                    }
                } else if(blockOp->getOperationType() == Operations::Operation::OperationType::IfOp) {
                    auto ifOp = static_pointer_cast<Operations::IfOperation>(blockOp);
                    if(replacedOperations.contains(ifOp->getBooleanValue()->getIdentifier())) {
                        ifOp->setBooleanValue(replacedOperations.at(ifOp->getBooleanValue()->getIdentifier()));
                    }
                    if(!visitedBlocks.contains(ifOp->getTrueBlockInvocation().getBlock()->getIdentifier())) {
                        newBlocks.emplace(ifOp->getTrueBlockInvocation().getBlock());
                    }
                    if(ifOp->hasFalseCase() && !visitedBlocks.contains(ifOp->getFalseBlockInvocation().getBlock()->getIdentifier())) {
                        newBlocks.emplace(ifOp->getFalseBlockInvocation().getBlock());
                    }
                } // else: is returnOp
            }
            ++blockOpIndex;
        }
    } while (!newBlocks.empty());
}

}//namespace NES::Nautilus::IR