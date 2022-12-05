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

#include "Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp"
#include "Nautilus/IR/Operations/ConstIntOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp"
#include "Nautilus/IR/Operations/Loop/LoopInfo.hpp"
#include "Nautilus/IR/Operations/Loop/LoopOperation.hpp"
#include "Nautilus/Util/IRDumpHandler.hpp"
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Phases/StructuredControlFlowPhase.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <unordered_map>

using namespace NES::Nautilus::IR::Operations;
namespace NES::Nautilus::IR {

void StructuredControlFlowPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = StructuredControlFlowPhaseContext(std::move(ir));
    phaseContext.process();
};

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    findLoopHeadBlocks(rootOperation->getFunctionBasicBlock());
    createIfOperations(rootOperation->getFunctionBasicBlock());
}

void findAndAddConstantOperations(BasicBlockPtr& currentBlock, 
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

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::checkBranchForLoopHeadBlocks(
        IR::BasicBlockPtr& currentBlock, std::stack<IR::BasicBlockPtr>& candidates, 
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
            candidates.emplace(currentBlock);
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
        if(!currentBlock->isLoopHeaderBlock() && currentBlock->getPredecessors().size() == 2) {
            // We do not support loops with iteration variables (vars that are manipulated in the loop) yet
            // At this point, we reached the loop-header-block coming from one of its loopEndBlocks

            // 1. get comparison-operation(compOp) from loop-header-block
            // Currently, we are not supporting >= and <=, since they are separated into 3 comparison operations.
            // -> We can detect <= and >= by checking if the CompareOp is an 'or' op
            // -> furthermore, we can check whether the 4th last operation is a '<' or a '>'
            if(ifOp->getBooleanValue()->getOperationType() == Operation::CompareOp) {
                // 2. store LHS and RHS from compOp) -> one is the lowerBound&inductionVar, the other is the upperBound
                auto compareOp = std::static_pointer_cast<Operations::CompareOperation>(ifOp->getBooleanValue());
                // 3. get the second-to-last operation from the loopEndBlock (incrementOp)
                auto incrementOp = priorBlock->getOperations().at(priorBlock->getOperations().size() - 2);
                // Todo need to apply checks:
                // -> how to find out whether this is a counted loop?
                // -> must be arithmetic function && must be yielded to loop-header and used in compare op
                // todo make sure that its used in compareOp
                if((incrementOp->getOperationType() == Operation::AddOp || incrementOp->getOperationType() == Operation::SubOp)
                    && compareOp->getComparator() < Operations::CompareOperation::Comparator::FOLT 
                    && priorBlock->getTerminatorOp()->getOperationType() == Operation::BranchOp && 
                    std::static_pointer_cast<Operations::BranchOperation>(
                        priorBlock->getTerminatorOp())->getNextBlockInvocation().getOperationArgIndex(incrementOp) != -1) {
                    
                    // 4. find out the argument-index of the incrementOp (loopEndBlock argument in branchOp to loop-header-block)
                    auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(priorBlock->getTerminatorOp());
                    auto inductionVarArgIndex = branchOp->getNextBlockInvocation().getOperationArgIndex(incrementOp);
                    // 5. find out the corresponding argument name of the loop-header-block
                    auto loopInductionVarArg = currentBlock->getArguments().at(inductionVarArgIndex);
                    // 6. check whether LHS or RHS of compOp correspond to the argument name found in (2.)
                    std::shared_ptr<Operations::ConstIntOperation> inductionVar;
                    std::shared_ptr<Operations::ConstIntOperation> stopValue;
                    // OperationPtr stopValue;
                    bool countedLoopDetected = true;
                    bool inductionVarLessThanSide = true;
                    if(compareOp->getLeftInput() == loopInductionVarArg) {
                        inductionVar = constantValues[compareOp->getLeftInput()->getIdentifier()];
                        stopValue = constantValues[compareOp->getRightInput()->getIdentifier()];
                        // Determine whether the induction var is on the smaller or greater side //todo handle equals
                        if( compareOp->getComparator() == Operations::CompareOperation::ISGE || 
                            compareOp->getComparator() == Operations::CompareOperation::ISGT || 
                            compareOp->getComparator() == Operations::CompareOperation::IUGE || 
                            compareOp->getComparator() == Operations::CompareOperation::IUGT) 
                        {
                            inductionVarLessThanSide = false;
                        } // else it must be greater
                    } else if(compareOp->getRightInput() == loopInductionVarArg){
                        inductionVar = constantValues[compareOp->getRightInput()->getIdentifier()];
                        stopValue = constantValues[compareOp->getLeftInput()->getIdentifier()];
                        // Determine whether the induction var is on the smaller or greater side
                        if( compareOp->getComparator() == Operations::CompareOperation::ISLE || 
                            compareOp->getComparator() == Operations::CompareOperation::ISLT || 
                            compareOp->getComparator() == Operations::CompareOperation::IULE || 
                            compareOp->getComparator() == Operations::CompareOperation::IULT) 
                        {
                            inductionVarLessThanSide = false;
                        } // else it must be greater
                    } else {
                        countedLoopDetected = false;
                    }
                    // 7. check incrementOp:
                    if(countedLoopDetected) {
                        std::shared_ptr<Operations::ConstIntOperation> stepSize;
                        switch(incrementOp->getOperationType()) {
                            case Operation::AddOp: {
                                auto incrementOpAdd = std::static_pointer_cast<Operations::AddOperation>(incrementOp);
                                stepSize = (constantValues[incrementOpAdd->getLeftInput()->getIdentifier()] == 
                                            constantValues[loopInductionVarArg->getIdentifier()])
                                    ? constantValues[incrementOpAdd->getRightInput()->getIdentifier()] 
                                    : constantValues[incrementOpAdd->getLeftInput()->getIdentifier()];
                                break;
                            }
                            case Operation::SubOp: {
                                auto incrementOpAdd = std::static_pointer_cast<Operations::SubOperation>(incrementOp);
                                stepSize = (constantValues[incrementOpAdd->getLeftInput()->getIdentifier()] == 
                                            constantValues[loopInductionVarArg->getIdentifier()])
                                    ? constantValues[incrementOpAdd->getRightInput()->getIdentifier()] 
                                    : constantValues[incrementOpAdd->getLeftInput()->getIdentifier()];
                                break;
                            }
                            // We do not support mul and div, because MLIR for-loop-ops require a fixed step size.
                            default:
                                NES_ERROR("StructuredControlFlowPass::checkBranchForLoopHeadBlocks: Could not create LoopInfo." <<
                                        "Provided operation type: " << incrementOp->getOperationType() << "is not supported.");
                                NES_NOT_IMPLEMENTED(); //Todo this throws exception. We might want to handle this case gracefully and simply abort checking for for-loops
                                
                        }
                        // Todo stepsize && upperBound depends on comparison type, and on arithmetic function
                        // Assumption: indVar(lowerBound) and upperBound are set up correctly
                        // Assumption: MLIR SCF's upperBound is an open interval (0->3) means 3 steps, not 4 (correct!)
                        // Fact: MLIR requires -index- types for lower- and upperBound. index types are
                        // -> target-specific e.g. 32 or 64-bit 'ints' -> we do not account for e.g. float types
                        // Problem: what if we add a negative number?
                        // -> same for other arithmetic operations (div, sub, mul)
                        // Solution: normalize function (normalizeLoopCondition)
                        // -> takes: indVar, stopValue(upperBound), comparisonType, arithmeticFunction, stepSize
                        // -> checks if sign of arithmeticFunction.LHS and/or arithmeticFunction.RHS is negative
                        // ----> use it to determine whether the indVar and/or the stepSize is negative
                        // -> 2 options: 
                        // Todo do we need to check the comparison direction?
                        //  -> indVar >= stopVal && indVar > stopVal -> decreasing
                        //  -> indVar >= stopVal && indVar < stopVal -> will not loop
                        //      1. either the indVar decreases towards the stopValue
                        //      -> indVar >= stopValue && arithOp is (sub && stepSize is positive) or (add && stepSize is negative)
                        //      -> lowerBound = stopValue; upperBound = indVar(+1); stepSize = stepSize * stepSizeIsPositive
                        //      ----> upperBound = indVar + 1 if comparison not '>'
                        //      2. or the indVar increases towards the stopValue
                        //      -> indVar <= stopValue && arithOp is (sub && stepSize is negative) or (add && stepSize is positive)
                        //      -> lowerBound = indVar; upperBound = stopValue; stepSize = stepSize * stepSizeIsPositive
                        //      ----> upperBound = stopValue - 1 if comparison not '<'
                        // !!! -> can only detect counted loop if countOperation is add or sub
                        // Decreasing (indVar is reduced in each step -> upperBound=indVar, lowerBound = stopValue)
                        // Todo can a loop be decreasing if indVar <= stopValue?
                        // -> no
                        // -> if indVar == stopValue -> could not detect loop

                        auto countedLoopInfo = std::make_unique<CountedLoopInfo>();
                        // Decreasing Case
                        if(inductionVar->getConstantIntValue() > stopValue->getConstantIntValue()) {
                            // Check if step size matches decreasing case
                            if(!inductionVarLessThanSide &&
                                ((incrementOp->getOperationType() == Operation::SubOp && stepSize->getConstantIntValue() >= 0)
                                || (incrementOp->getOperationType() == Operation::AddOp && stepSize->getConstantIntValue() < 0))) {
                                countedLoopInfo->lowerBound = stopValue->getConstantIntValue();
                                countedLoopInfo->stepSize = stepSize->getConstantIntValue() * ( 1 - (2*(stepSize->getConstantIntValue() < 0)));
                                countedLoopInfo->upperBound = inductionVar->getConstantIntValue() 
                                    + ((compareOp->getComparator() != CompareOperation::ISGT) && (compareOp->getComparator() != CompareOperation::IUGT)
                                        && (compareOp->getComparator() != CompareOperation::ISLT) && (compareOp->getComparator() != CompareOperation::IULT));
                            } else {
                                countedLoopDetected = false; //Todo add debug msg !! could increment edgeCounter first, and then add returns here
                            }
                        // Increasing Case
                        } else if (inductionVar->getConstantIntValue() < stopValue->getConstantIntValue()) {
                            // Check if step size matches decreasing case
                            if(inductionVarLessThanSide &&
                                ((incrementOp->getOperationType() == Operation::SubOp && stepSize->getConstantIntValue() <= 0)
                                || (incrementOp->getOperationType() == Operation::AddOp && stepSize->getConstantIntValue() > 0))) {
                                countedLoopInfo->lowerBound = inductionVar->getConstantIntValue();
                                countedLoopInfo->stepSize = stepSize->getConstantIntValue() * ( 1 - (2*(stepSize->getConstantIntValue() < 0)));
                                //Todo only checking for comparators one-sided
                                // -> need to check GT if 
                                countedLoopInfo->upperBound = stopValue->getConstantIntValue() 
                                    + ((compareOp->getComparator() != CompareOperation::ISLT) && (compareOp->getComparator() != CompareOperation::IULT)
                                        && (compareOp->getComparator() != CompareOperation::ISGT) && (compareOp->getComparator() != CompareOperation::IUGT));
                            } else {
                                countedLoopDetected = false; //Todo add debug msg
                            }
                        } else { 
                            // induction variable == stop variable -> no loop can be detected
                            countedLoopDetected = false; //Todo add debug msg
                        }
                        if(countedLoopDetected) {
                            // countedLoopInfo->loopInitialIteratorArguments = ?;
                            // countedLoopInfo->loopBodyIteratorArguments = ?;
                            // countedLoopInfo->loopBodyInductionVariable = inductionVar;
                            // Todo REMOVE body block and false block from countedLoopInfo
                            // -> could also remove loopEndBlock
                            // INSTEAD safe it directly in loopOperation
                            // countedLoopInfo->loopBodyBlock = ifOp->getTrueBlockInvocation().getBlock();
                            // countedLoopInfo->loopFalseBlock = ifOp->getFalseBlockInvocation().getBlock();
                            countedLoopInfo->loopEndBlock = std::move(priorBlock);
                            // Create LoopOperation with CountedLoopInfo
                            loopOp->setLoopType(Operations::LoopOperation::LoopType::CountedLoop);
                            loopOp->setLoopInfo(std::move(countedLoopInfo));
                        } else {
                            // loop-count-operation is not used in compare-operation. Cannot detect counted loop.
                            NES_DEBUG("Could not detect counted loop.");
                        }
                    } else {
                        // loop-count-operation is not used in compare-operation. Cannot detect counted loop.
                        NES_DEBUG("Could not detect counted loop.");
                    }
                } else {
                    // second-to-last-operation in loop-end-block either was no arithmetic operation or was an arithmetic
                    // operation, but is not yielded to loop header and used in compare op.
                    NES_DEBUG("Could not detect counted loop.");
                }
            } else {
                // loop-header does not use a comparison operation for boolean value.
                NES_DEBUG("Loop header without comparison operation not supported.");
            }
        }
        // Todo we should at least convert the if-operation to a loop operation
        loopOp->getLoopHeadBlock().setBlock(currentBlock);
        loopOp->getLoopBodyBlock().setBlock(ifOp->getTrueBlockInvocation().getBlock());
        loopOp->getLoopFalseBlock().setBlock(ifOp->getFalseBlockInvocation().getBlock());
        currentBlock->replaceTerminatorOperation(std::move(loopOp));
        // Currently, we increase the number of loopBackEdges (default: 0) of the loop-header-block.
        // Thus, after this phase, if a block has numLoopBackEdges > 0, it is a loop-header-block.
        // More information will be added in the scope of issue #3169.
        currentBlock->incrementNumLoopBackEdge();
    }
}

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::findLoopHeadBlocks(IR::BasicBlockPtr currentBlock) {
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

bool StructuredControlFlowPhase::StructuredControlFlowPhaseContext::mergeBlockCheck(
    IR::BasicBlockPtr& currentBlock,
    std::stack<std::unique_ptr<IfOpCandidate>>& ifOperations,
    std::unordered_map<std::string, uint32_t>& numMergeBlocksVisits,
    bool newVisit,
    const std::unordered_set<IR::BasicBlockPtr>& loopBlockWithVisitedBody) {
    uint32_t openEdges = 0;
    uint32_t numPriorVisits = 0;
    bool mergeBlockFound = false;

    bool isAlreadyVisitedMergeBlock = numMergeBlocksVisits.contains(currentBlock->getIdentifier());
    if (isAlreadyVisitedMergeBlock) {
        // We deduct '1' from the number of prior visits so that openEdges is > 1 even if we already visited all the
        // merge-blocks's open edges. This is important to not accidentally recognize branch-blocks(openEdges: 1) as
        // merge-blocks.
        numPriorVisits = numMergeBlocksVisits.at(currentBlock->getIdentifier()) - 1;
    }
    // Calculating openEdges:
    //  If we did not loop back to a loop-block coming from the loop-block's body:
    //  -> deduct the number of prior visits from the number of predecessors of the block.
    //  -> if it is a loop-header-block, deduct the number of loopBackEdges.
    //  Else:
    //  -> simply deduct the number of prior visits from the number of loopBackEdges.
    //  -> if the loop-header-block has no more openEdges, we exhausted its true-branch and switch to its false-branch.
    if (!loopBlockWithVisitedBody.contains(currentBlock)) {
        openEdges = currentBlock->getPredecessors().size() - numPriorVisits
            - (currentBlock->isLoopHeaderBlock()) * currentBlock->getNumLoopBackEdges();
    } else {
        openEdges = currentBlock->getNumLoopBackEdges() - numPriorVisits;
        if(openEdges < 2) {
                // We exhausted the loop-operations true-branch (body block) and now switch to its false-branch.
                currentBlock = std::static_pointer_cast<Operations::LoopOperation>(currentBlock->getTerminatorOp())->getLoopFalseBlock().getBlock();
                // Since we switched to a new currentBlock, we need to check whether it is a merge-block with openEdges.
                // If the new currentBlock is a loop-header-block again, we have multiple recursive calls.
                return mergeBlockCheck(currentBlock, ifOperations, numMergeBlocksVisits, true, loopBlockWithVisitedBody);
        }
    }
    // If the number of openEdges is 2 or greater, we found a merge-block.
    mergeBlockFound = openEdges > 1;
    // If we found a merge-block, and we came from a new edge increase the visit counter by 1 or set it to 1.
    if (mergeBlockFound && newVisit && isAlreadyVisitedMergeBlock) {
        numMergeBlocksVisits[currentBlock->getIdentifier()] = numMergeBlocksVisits[currentBlock->getIdentifier()] + 1;
    } else if (mergeBlockFound && newVisit && !isAlreadyVisitedMergeBlock) {
        numMergeBlocksVisits.emplace(std::pair{currentBlock->getIdentifier(), 1});
    }
    return mergeBlockFound;
}

void StructuredControlFlowPhase::StructuredControlFlowPhaseContext::createIfOperations(IR::BasicBlockPtr currentBlock) {
    std::stack<std::unique_ptr<IfOpCandidate>> ifOperations;
    std::stack<IR::BasicBlockPtr> mergeBlocks;
    std::unordered_map<std::string, uint32_t> numMergeBlockVisits;
    std::unordered_set<IR::BasicBlockPtr> loopBlockWithVisitedBody;
    bool mergeBlockFound = true;
    // The newVisit flag is passed to the mergeBlockCheck() function to indicate whether we traversed a new edge to the
    // currentBlock before calling mergeBlockCheck().
    bool newVisit = true;

    // Iterate over graph until all if-operations have been processed and matched with their corresponding merge-blocks.
    while (mergeBlockFound) {
        // Check blocks (DFS) until an open merge-block was found. Push encountered if-operations to stack.
        // In a nutshell, we identify open merge-blocks by checking the number of incoming edges vs mergeBlockNumVisits
        // and numLoopBackEdges. For example, a block that has 2 incoming edges, 2 numMergeBlockVisits, and 0
        // numLoopBackEdges is an closed merge-block that merges two control-flow-branches. In contrast, a block that has
        // 5 incoming edges, 2 numMergeBlockVisits, and 1 numLoopBackEdges is an open merge-block with still 2 open
        // control-flow-merge-edges. Also, it is a loop-header-block with 1 numLoopBackEdge. (5-2-1 => 2 still open)
        while (!(mergeBlockFound =
                     mergeBlockCheck(currentBlock, ifOperations, numMergeBlockVisits, newVisit, loopBlockWithVisitedBody))
               && (currentBlock->getTerminatorOp()->getOperationType() != Operation::ReturnOp)) {
            auto terminatorOp = currentBlock->getTerminatorOp();
            if (terminatorOp->getOperationType() == Operation::BranchOp) {
                // If the currentBlock is a simple branch-block, we move to the nextBlock.
                auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
                currentBlock = branchOp->getNextBlockInvocation().getBlock();
                newVisit = true;
            } else if (terminatorOp->getOperationType() == Operation::IfOp) { 
                // If the currentBlock is an if-block, we push its if-operation on top of our IfOperation stack.
                auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
                ifOperations.emplace(std::make_unique<IfOpCandidate>(IfOpCandidate{ifOp, true}));
                // We now follow the if-operation's true-branch until we find a new if-operation or a merge-block.
                currentBlock = ifOp->getTrueBlockInvocation().getBlock();
                newVisit = true;
            }
            else if (terminatorOp->getOperationType() == Operation::LoopOp) {
                loopBlockWithVisitedBody.emplace(currentBlock);
                if(numMergeBlockVisits.contains(currentBlock->getIdentifier())) {
                    numMergeBlockVisits.erase(currentBlock->getIdentifier());
                }
                auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(terminatorOp);
                currentBlock = loopOp->getLoopBodyBlock().getBlock();
                newVisit = true;
            }
        }
        // If no merge-block was found, we traversed the entire graph and are done (return block is current block).
        if (mergeBlockFound) {
            // If a merge-block was found, depending on whether the we are in the current if-operations' true
            // or false branch, we either set it as the current if-operation's true-branch-block,
            // or set it as current if-operation's false-branch-block.
            if (ifOperations.top()->isTrueBranch) {
                // We explored the current if-operation's true-branch and now switch to its false-branch.
                ifOperations.top()->isTrueBranch = false;
                mergeBlocks.emplace(currentBlock);
                currentBlock = ifOperations.top()->ifOp->getFalseBlockInvocation().getBlock();
                newVisit = true;
            } else {
                // Make sure that we found the current merge-block for the current if-operation.
                assert(mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier());
                // Set currentBlock as merge-block for the current if-operation and all if-operations on the stack that:
                //  1. Are in their false-branches (their merge-block was pushed to the stack).
                //  2. Have a corresponding merge-block on the stack that matches the currentBlock.
                do {
                    auto mergeBlock = std::move(mergeBlocks.top());
                    ifOperations.top()->ifOp->setMergeBlock(std::move(mergeBlock));
                    mergeBlocks.pop();
                    ifOperations.pop();
                } while (!ifOperations.empty() && !ifOperations.top()->isTrueBranch && !mergeBlocks.empty()
                         && mergeBlocks.top()->getIdentifier() == currentBlock->getIdentifier());
                // In this case, we do not visit a block via a new edge, so newVisit is false.
                // This is important in case the the current top-most if-operation on the stack is in its true-branch
                // and the currentBlock is its merge-block.
                newVisit = false;
            }
        }
    }
}

}//namespace NES::Nautilus::IR