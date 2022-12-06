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

#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopInfo.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/Util/IRDumpHandler.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Phases/LoopDetectionPhase.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <unordered_map>

using namespace NES::Nautilus::IR::Operations;
namespace NES::Nautilus::IR {

void LoopDetectionPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = LoopDetectionPhaseContext(std::move(ir));
    phaseContext.process();
};

void LoopDetectionPhase::LoopDetectionPhaseContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    findLoopHeadBlocks(rootOperation->getFunctionBasicBlock());
}

void LoopDetectionPhase::LoopDetectionPhaseContext::findAndAddConstantOperations(BasicBlockPtr& currentBlock, 
        std::unordered_map<std::string, std::shared_ptr<Operations::ConstIntOperation>>& constantValues) {
    for(auto operation : currentBlock->getOperations()) {
        if(operation->getOperationType() == Operation::ConstIntOp) {
            auto constIntOp = std::static_pointer_cast<Operations::ConstIntOperation>(operation);
            //Works because ConstInts have identifiers (only ifs and brs do not)
            constantValues.emplace(std::make_pair(constIntOp->getIdentifier(), constIntOp));
            if(currentBlock->getTerminatorOp()->getOperationType() == Operation::BranchOp) {
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(currentBlock->getTerminatorOp());
                int argIndex = branchOp->getNextBlockInvocation().getOperationArgIndex(constIntOp);
                if(argIndex != -1) {
                    // If ConstantInt is passed as argument to nextBlock, find out the name of the ConstantInt in nextBlock.
                    auto nextBlockArgName = branchOp->getNextBlockInvocation().getBlock()->getArguments().at(argIndex);
                    constantValues.emplace(std::make_pair(nextBlockArgName->getIdentifier(), constIntOp));
                }
            } else {
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
                int argIndex = ifOp->getTrueBlockInvocation().getOperationArgIndex(constIntOp);
                if(argIndex != -1) {
                    // If ConstantInt is passed as argument to nextBlock, find out the name of the ConstantInt in nextBlock.
                    auto nextBlockArgName = ifOp->getTrueBlockInvocation().getBlock()->getArguments().at(argIndex);
                    constantValues.emplace(std::make_pair(nextBlockArgName->getIdentifier(), constIntOp));
                }
                argIndex = ifOp->getFalseBlockInvocation().getOperationArgIndex(constIntOp);
                if(argIndex != -1) {
                    // If ConstantInt is passed as argument to nextBlock, find out the name of the ConstantInt in nextBlock.
                    auto nextBlockArgName = ifOp->getFalseBlockInvocation().getBlock()->getArguments().at(argIndex);
                    constantValues.emplace(std::make_pair(nextBlockArgName->getIdentifier(), constIntOp));
                }
            }
        }
    }
    if(currentBlock->getTerminatorOp()->getOperationType() == Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(currentBlock->getTerminatorOp());
        for(auto arg : currentBlock->getArguments()) {
            if(constantValues.contains(arg->getIdentifier())) {
                int argIndex = branchOp->getNextBlockInvocation().getOperationArgIndex(arg);
                if(argIndex != -1) {
                    auto nextBlockArgName = branchOp->getNextBlockInvocation().getBlock()->getArguments().at(argIndex);
                    constantValues.emplace(std::make_pair(nextBlockArgName->getIdentifier(), constantValues[arg->getIdentifier()]));
                }
            }
        }
    } else {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
        for(auto arg : currentBlock->getArguments()) {
            if(constantValues.contains(arg->getIdentifier())) {
                int argIndex = ifOp->getTrueBlockInvocation().getOperationArgIndex(arg);
                if(argIndex != -1) {
                    auto nextBlockArgName = ifOp->getTrueBlockInvocation().getBlock()->getArguments().at(argIndex);
                    constantValues.emplace(std::make_pair(nextBlockArgName->getIdentifier(), constantValues[arg->getIdentifier()]));
                }
            }
        }
        for(auto arg : currentBlock->getArguments()) {
            if(constantValues.contains(arg->getIdentifier())) {
                int argIndex = ifOp->getFalseBlockInvocation().getOperationArgIndex(arg);
                if(argIndex != -1) {
                    auto nextBlockArgName = ifOp->getFalseBlockInvocation().getBlock()->getArguments().at(argIndex);
                    constantValues.emplace(std::make_pair(nextBlockArgName->getIdentifier(), constantValues[arg->getIdentifier()]));
                }
            }
        }
    }
}

void LoopDetectionPhase::LoopDetectionPhaseContext::checkBranchForLoopHeadBlocks(
        IR::BasicBlockPtr& currentBlock, std::stack<IR::BasicBlockPtr>& ifBlocks, 
        std::unordered_set<std::string>& visitedBlocks, std::unordered_set<std::string>& loopHeaderCandidates,
        IR::BasicBlockPtr& priorBlock, std::unordered_map<std::string, std::shared_ptr<Operations::ConstIntOperation>>& constantValues) {
    // Follow the true-branch of the current, and all nested if-operations until either
    // currentBlock is an already visited block, or currentBlock is the return-block.
    // Newly encountered if-operations are added as loopHeadCandidates.
    while(!visitedBlocks.contains(currentBlock->getIdentifier()) && 
            currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp) {
        findAndAddConstantOperations(currentBlock, constantValues);
        auto terminatorOp = currentBlock->getTerminatorOp();
        if(terminatorOp->getOperationType() == Operation::BranchOp) {
            auto nextBlock = std::static_pointer_cast<IR::Operations::BranchOperation>(
                terminatorOp)->getNextBlockInvocation().getBlock();
            visitedBlocks.emplace(currentBlock->getIdentifier());
            priorBlock = currentBlock;
            currentBlock = nextBlock;
        } else if (terminatorOp->getOperationType() == Operation::IfOp) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            loopHeaderCandidates.emplace(currentBlock->getIdentifier());
            ifBlocks.emplace(currentBlock);
            visitedBlocks.emplace(currentBlock->getIdentifier());
            priorBlock = currentBlock;
            currentBlock = ifOp->getTrueBlockInvocation().getBlock();
        }
    }
    // If currentBlock is an already visited block that also is a loopHeaderCandidate, we found a loop-header-block.
    if(loopHeaderCandidates.contains(currentBlock->getIdentifier())) {
        // If loop-header-block has not been visited before and only has two predecessors, create counted-for-loop.
        // Todo how to avoid while loops with two predecessors that are not counted?
        // -> whether we can detect counted loops depends on whether the last block contains the count-operation (as second to last)
        // -> and it depends on the comparison types
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
        auto loopOp = std::make_shared<Operations::LoopOperation>(Operations::LoopOperation::LoopType::DefaultLoop);
        loopOp->getLoopHeadBlock().setBlock(currentBlock);
        loopOp->getLoopBodyBlock().setBlock(ifOp->getTrueBlockInvocation().getBlock());
        loopOp->getLoopFalseBlock().setBlock(ifOp->getFalseBlockInvocation().getBlock());
        currentBlock->replaceTerminatorOperation(loopOp);
        // currentBlock->replaceTerminatorOperation(std::move(loopOp));

        // Currently, we increase the number of loopBackEdges (default: 0) of the loop-header-block.
        // Thus, after this phase, if a block has numLoopBackEdges > 0, it is a loop-header-block.
        currentBlock->incrementNumLoopBackEdge();
        // Counted Loop Detection
        if(currentBlock->getNumLoopBackEdges() < 2 && currentBlock->getPredecessors().size() == 2) {
            // We do not support loops with iteration variables (vars that are manipulated in the loop) yet
            // Currently, we are not supporting >= and <=, since they are separated into 3 comparison operations.
            // -> We can detect <= and >= by checking if the CompareOp is an 'or' op
            // -> furthermore, we can check whether the 4th last operation is a '<' or a '>'
            // Also, not equal leads to a 'not' operation in the boolean value.
            // At this point, we reached the loop-header-block coming from one of its loopEndBlocks
            // 1. get comparison-operation(compOp) from loop-header-block
            if(ifOp->getBooleanValue()->getOperationType() == Operation::CompareOp) {
                // 2. store LHS and RHS from compOp) -> one is the lowerBound&inductionVar, the other is the upperBound
                auto compareOp = std::static_pointer_cast<Operations::CompareOperation>(ifOp->getBooleanValue());
                // 3. get the second-to-last operation from the loopEndBlock (incrementOp)
                auto countOp = priorBlock->getOperations().at(priorBlock->getOperations().size() - 2);
                if(compareOp->getComparator() != Operations::CompareOperation::IEQ
                    && (countOp->getOperationType() == Operation::AddOp || countOp->getOperationType() == Operation::SubOp)
                    && compareOp->getComparator() < Operations::CompareOperation::Comparator::FOLT 
                    && priorBlock->getTerminatorOp()->getOperationType() == Operation::BranchOp
                    && std::static_pointer_cast<Operations::BranchOperation>(priorBlock->getTerminatorOp())
                        ->getNextBlockInvocation().getOperationArgIndex(countOp) != -1) {               
                    // 4. find out the argument-index of the incrementOp (loopEndBlock argument in branchOp to loop-header-block)
                    auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(priorBlock->getTerminatorOp());
                    auto inductionVarArgIndex = branchOp->getNextBlockInvocation().getOperationArgIndex(countOp);
                    // 5. find out the corresponding argument name of the loop-header-block
                    auto loopInductionVarArg = currentBlock->getArguments().at(inductionVarArgIndex);
                    // 6. check whether LHS or RHS of compOp correspond to the argument name found in (2.)
                    std::shared_ptr<Operations::ConstIntOperation> inductionVar;
                    std::shared_ptr<Operations::ConstIntOperation> stopValue;
                    if(compareOp->getLeftInput() == loopInductionVarArg) {
                        inductionVar = constantValues[compareOp->getLeftInput()->getIdentifier()];
                        stopValue = constantValues[compareOp->getRightInput()->getIdentifier()];
                        // Determine whether the induction var is on the smaller or greater side 
                        //todo handle equals
                        // -> remove GE? -> no
                        // if( compareOp->getComparator() == Operations::CompareOperation::ISGE || 
                        //     compareOp->getComparator() == Operations::CompareOperation::ISGT || 
                        //     compareOp->getComparator() == Operations::CompareOperation::IUGE || 
                        //     compareOp->getComparator() == Operations::CompareOperation::IUGT) 
                        // {
                        //     inductionVarIsOnLessThanSide = false;
                        // } // else it must be greater
                    } else if(compareOp->getRightInput() == loopInductionVarArg){
                        inductionVar = constantValues[compareOp->getRightInput()->getIdentifier()];
                        stopValue = constantValues[compareOp->getLeftInput()->getIdentifier()];
                        // Determine whether the induction var is on the smaller or greater side
                        // if( compareOp->getComparator() == Operations::CompareOperation::ISLE || 
                        //     compareOp->getComparator() == Operations::CompareOperation::ISLT || 
                        //     compareOp->getComparator() == Operations::CompareOperation::IULE || 
                        //     compareOp->getComparator() == Operations::CompareOperation::IULT) 
                        // {
                        //     inductionVarIsOnLessThanSide = false;
                        // } // else it must be greater
                    } else {
                        NES_DEBUG("Could not detect counted loop. The loop induction variable is not part of " << 
                                    "the loop-header comparison operation.")
                        return;
                    }
                    // 7. check incrementOp:
                    std::shared_ptr<Operations::ConstIntOperation> stepSize;
                    switch(countOp->getOperationType()) {
                        case Operation::AddOp: {
                            auto incrementOpAdd = std::static_pointer_cast<Operations::AddOperation>(countOp);
                            stepSize = (constantValues[incrementOpAdd->getLeftInput()->getIdentifier()] == 
                                        constantValues[loopInductionVarArg->getIdentifier()])
                                ? constantValues[incrementOpAdd->getRightInput()->getIdentifier()] 
                                : constantValues[incrementOpAdd->getLeftInput()->getIdentifier()];
                            break;
                        }
                        case Operation::SubOp: {
                            auto incrementOpAdd = std::static_pointer_cast<Operations::SubOperation>(countOp);
                            stepSize = (constantValues[incrementOpAdd->getLeftInput()->getIdentifier()] == 
                                        constantValues[loopInductionVarArg->getIdentifier()])
                                ? constantValues[incrementOpAdd->getRightInput()->getIdentifier()] 
                                : constantValues[incrementOpAdd->getLeftInput()->getIdentifier()];
                            break;
                        }
                        // We do not support mul and div, because MLIR for-loop-ops require a fixed step size.
                        default:
                            NES_DEBUG("Could not detect counted loop." <<
                                    "Provided operation type: " << countOp->getOperationType() << "is not supported.");
                            return;
                    }
                    auto countedLoopInfo = std::make_unique<CountedLoopInfo>();
                    // Decreasing Case
                    if(inductionVar->getConstantIntValue() > stopValue->getConstantIntValue()) {
                        // Check if step size matches decreasing case (indVar must be on the greater than side)
                        if(((countOp->getOperationType() == Operation::SubOp && stepSize->getConstantIntValue() >= 0)
                            || (countOp->getOperationType() == Operation::AddOp && stepSize->getConstantIntValue() < 0))) {
                            countedLoopInfo->lowerBound = stopValue->getConstantIntValue();
                            countedLoopInfo->stepSize = stepSize->getConstantIntValue() * ( 1 - (2*(stepSize->getConstantIntValue() < 0)));
                            countedLoopInfo->upperBound = inductionVar->getConstantIntValue() 
                                + ((compareOp->getComparator() != CompareOperation::ISGT) && (compareOp->getComparator() != CompareOperation::IUGT)
                                    && (compareOp->getComparator() != CompareOperation::ISLT) && (compareOp->getComparator() != CompareOperation::IULT));
                        } else {
                            NES_DEBUG("Could not detect counted loop. Found decreasing loop (loop induction variable < loop stop variable), but " <<
                                      "the step size is positive.");
                            return;
                        }
                    // Increasing Case
                    } else if (inductionVar->getConstantIntValue() < stopValue->getConstantIntValue()) {
                        // Check if step size matches decreasing case
                        if(((countOp->getOperationType() == Operation::SubOp && stepSize->getConstantIntValue() <= 0)
                            || (countOp->getOperationType() == Operation::AddOp && stepSize->getConstantIntValue() > 0))) {
                            countedLoopInfo->lowerBound = inductionVar->getConstantIntValue();
                            countedLoopInfo->stepSize = stepSize->getConstantIntValue() * ( 1 - (2*(stepSize->getConstantIntValue() < 0)));
                            countedLoopInfo->upperBound = stopValue->getConstantIntValue() 
                                + ((compareOp->getComparator() != CompareOperation::ISLT) && (compareOp->getComparator() != CompareOperation::IULT)
                                    && (compareOp->getComparator() != CompareOperation::ISGT) && (compareOp->getComparator() != CompareOperation::IUGT));
                        } else {
                            NES_DEBUG("Could not detect counted loop. Found increasing loop (loop induction variable > loop stop variable), but " <<
                                      "the step size is negative.");
                            return;
                        }
                    } else { 
                        NES_DEBUG("Could not detect a counted loop. Reason: UpperBound == LowerBound.");
                        return;
                    }
                    countedLoopInfo->loopEndBlock = std::move(priorBlock);
                    // Create LoopOperation with CountedLoopInfo
                    loopOp->setLoopType(Operations::LoopOperation::LoopType::CountedLoop);
                    loopOp->setLoopInfo(std::move(countedLoopInfo));
                } else {
                    // second-to-last-operation in loop-end-block either was no arithmetic operation or was an arithmetic
                    // operation, but is not yielded to loop header and used in compare op.
                    NES_DEBUG("Could not detect counted loop. Possible reasons: \n" 
                                << "1. The count-operation is not an addition or a subtraction.\n"
                                << "2. The loop-header comparison uses floating point types.\n"
                                << "3. The loop-end-block does not use a branch-operation to loop back\n"
                                << "4. The result of the count-operation is not an argument of the loop-header.\n"
                                << "5. The compare operation uses an equal comparator, which we do not support.\n");
                }
            } else {
                // loop-header does not use a comparison operation for boolean value.
                NES_DEBUG("Loop header without comparison operation not supported. This currently includes '>=' and '<='");
            }
        }
    }
}

void LoopDetectionPhase::LoopDetectionPhaseContext::findLoopHeadBlocks(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> ifBlocks;
    std::unordered_set<std::string> loopHeaderCandidates;
    std::unordered_set<std::string> visitedBlocks;
    std::unordered_map<std::string, std::shared_ptr<Operations::ConstIntOperation>> constantValues;
    IR::BasicBlockPtr priorBlock = currentBlock;

    bool returnBlockVisited = false;
    bool noMoreIfBlocks = true;
    // We iterate over the IR graph starting with currentBlock being the body of the root-operation.
    // We stop iterating when we have visited the return block at least once, and there are no more
    // unvisited if-blocks on the stack. If the IR graph is valid, no more unvisited if-operations exist.
    do {
        // Follow a branch through the query branch until currentBlock is either the return- or an already visited block.
        checkBranchForLoopHeadBlocks(currentBlock, ifBlocks, visitedBlocks, loopHeaderCandidates, priorBlock, constantValues);
        // Set the current values for the loop halting values.
        noMoreIfBlocks = ifBlocks.empty();
        returnBlockVisited = returnBlockVisited || (currentBlock->getTerminatorOp()->getOperationType() == Operation::ReturnOp);
        priorBlock = currentBlock;
        if(!noMoreIfBlocks) {
            // When we take the false-branch of an ifOperation, we completely exhausted its true-branch.
            // Since loops can only loop back on their true-branch, we can safely stop tracking it as a loop candidate.
            // Todo could replace this by simply storing state on whether we are in the current if-operation's true- or false-branch.
            loopHeaderCandidates.erase(ifBlocks.top()->getIdentifier());
            // Set currentBlock to first block in false-branch of ifOperation.
            // The false branch might contain nested loop-operations.
            if(ifBlocks.top()->getTerminatorOp()->getOperationType() == Operation::IfOp) {
                currentBlock = std::static_pointer_cast<IR::Operations::IfOperation>(ifBlocks.top()->getTerminatorOp())
                                ->getFalseBlockInvocation().getBlock();
            } else {
                currentBlock = std::static_pointer_cast<IR::Operations::LoopOperation>(ifBlocks.top()->getTerminatorOp())
                                ->getLoopFalseBlock().getBlock();
            }
            ifBlocks.pop();
        }
    } while (!(noMoreIfBlocks && returnBlockVisited));
}

}//namespace NES::Nautilus::IR