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

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopInfo.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Phases/LoopDetectionPhase.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Nautilus/Util/IRDumpHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <memory>
#include <stack>
#include <string>
#include <unordered_map>

namespace NES::Nautilus::IR {

void LoopDetectionPhase::apply(std::shared_ptr<IR::IRGraph> ir) {
    auto phaseContext = LoopDetectionPhaseContext(std::move(ir));
    phaseContext.process();
};

void LoopDetectionPhase::LoopDetectionPhaseContext::process() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    findLoopHeadBlocks(rootOperation->getFunctionBasicBlock());
}

using ConstIntOperationPtr = std::shared_ptr<IR::Operations::ConstIntOperation>;

std::pair<ConstIntOperationPtr, ConstIntOperationPtr> LoopDetectionPhase::LoopDetectionPhaseContext::getCompareOpConstants(
    const BasicBlockPtr& loopHeaderBlock,
    const BasicBlockPtr& loopBeforeBlock,
    const std::shared_ptr<Operations::CompareOperation>& compareOp) {
    std::pair<ConstIntOperationPtr, ConstIntOperationPtr> compareOpConstants;
    int leftInputArg = loopHeaderBlock->getIndexOfArgument(compareOp->getLeftInput());
    int rightInputArg = loopHeaderBlock->getIndexOfArgument(compareOp->getRightInput());
    if (loopBeforeBlock->getTerminatorOp()->getOperationType() == Operations::Operation::OperationType::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(loopBeforeBlock->getTerminatorOp());
        if (leftInputArg >= 0 || rightInputArg >= 0) {
            for (auto& operation : loopBeforeBlock->getOperations()) {
                if (leftInputArg >= 0
                    && operation->getIdentifier()
                        == branchOp->getNextBlockInvocation().getArguments().at(leftInputArg)->getIdentifier()
                    && operation->getOperationType() == Operations::Operation::OperationType::ConstIntOp) {
                    compareOpConstants.first = std::static_pointer_cast<Operations::ConstIntOperation>(operation);
                }
                if (rightInputArg >= 0
                    && operation->getIdentifier()
                        == branchOp->getNextBlockInvocation().getArguments().at(rightInputArg)->getIdentifier()
                    && operation->getOperationType() == Operations::Operation::OperationType::ConstIntOp) {
                    compareOpConstants.second = std::static_pointer_cast<Operations::ConstIntOperation>(operation);
                }
            }
        }
        if (leftInputArg <= 0 || rightInputArg <= 0) {
            for (auto& operation : loopHeaderBlock->getOperations()) {
                if (leftInputArg < 0 && operation->getIdentifier() == compareOp->getLeftInput()->getIdentifier()
                    && operation->getOperationType() == Operations::Operation::OperationType::ConstIntOp) {
                    compareOpConstants.first = std::static_pointer_cast<Operations::ConstIntOperation>(operation);
                }
                if (rightInputArg < 0 && operation->getIdentifier() == compareOp->getRightInput()->getIdentifier()
                    && operation->getOperationType() == Operations::Operation::OperationType::ConstIntOp) {
                    compareOpConstants.second = std::static_pointer_cast<Operations::ConstIntOperation>(operation);
                }
            }
        }
    }
    return compareOpConstants;
}

ConstIntOperationPtr LoopDetectionPhase::LoopDetectionPhaseContext::getStepSize(const BasicBlockPtr& loopEndBlock,
                                                                                const OperationPtr& countOp) {
    switch (countOp->getOperationType()) {
        case Operations::Operation::OperationType::AddOp: {
            auto incrementOpAdd = std::static_pointer_cast<Operations::AddOperation>(countOp);
            for (auto& operation : loopEndBlock->getOperations()) {
                if (incrementOpAdd->getLeftInput() == operation
                    && incrementOpAdd->getLeftInput()->getOperationType() == Operations::Operation::OperationType::ConstIntOp) {
                    return std::static_pointer_cast<Operations::ConstIntOperation>(incrementOpAdd->getLeftInput());
                } else if (incrementOpAdd->getRightInput() == operation
                           && incrementOpAdd->getRightInput()->getOperationType()
                               == Operations::Operation::OperationType::ConstIntOp) {
                    return std::static_pointer_cast<Operations::ConstIntOperation>(incrementOpAdd->getRightInput());
                }
            }
            break;
        }
        default: break;
    }
    return nullptr;
}

void LoopDetectionPhase::LoopDetectionPhaseContext::checkBranchForLoopHeadBlocks(
    IR::BasicBlockPtr& currentBlock,
    std::stack<IR::BasicBlockPtr>& ifBlocks,
    std::unordered_set<std::string>& visitedBlocks,
    std::unordered_set<std::string>& loopHeaderCandidates,
    IR::BasicBlockPtr& priorBlock) {
    // Follow the true-branch of the current, and all nested if-operations until either
    // currentBlock is an already visited block, or currentBlock is the return-block.
    // Newly encountered if-operations are added as loopHeadCandidates.
    while (!visitedBlocks.contains(currentBlock->getIdentifier())
           && currentBlock->getTerminatorOp()->getOperationType() != Operations::Operation::OperationType::ReturnOp) {
        // findAndAddConstantOperations(currentBlock, constantValues);
        auto terminatorOp = currentBlock->getTerminatorOp();
        if (terminatorOp->getOperationType() == Operations::Operation::OperationType::BranchOp) {
            auto nextBlock =
                std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp)->getNextBlockInvocation().getBlock();
            visitedBlocks.emplace(currentBlock->getIdentifier());
            priorBlock = currentBlock;
            currentBlock = nextBlock;
        } else if (terminatorOp->getOperationType() == Operations::Operation::OperationType::IfOp) {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            loopHeaderCandidates.emplace(currentBlock->getIdentifier());
            ifBlocks.emplace(currentBlock);
            visitedBlocks.emplace(currentBlock->getIdentifier());
            priorBlock = currentBlock;
            currentBlock = ifOp->getTrueBlockInvocation().getBlock();
        }
    }
    // If currentBlock is an already visited block that also is a loopHeaderCandidate, we found a loop-header-block.
    if (loopHeaderCandidates.contains(currentBlock->getIdentifier())) {
        currentBlock->incrementNumLoopBackEdge();
        // Loop header blocks always have an if-operation as their terminator operation.
        // But because we convert it to a loop-operation, the below condition is only true on the first visit.
        if (currentBlock->getTerminatorOp()->getOperationType() == Operations::Operation::OperationType::IfOp) {
            // We convert this if-operation to a general loop operation.
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(currentBlock->getTerminatorOp());
            auto loopOp = std::make_shared<Operations::LoopOperation>(Operations::LoopOperation::LoopType::DefaultLoop);
            loopOp->getLoopHeadBlock().setBlock(currentBlock);
            loopOp->getLoopBodyBlock().setBlock(ifOp->getTrueBlockInvocation().getBlock());
            loopOp->getLoopEndBlock().setBlock(priorBlock);
            // Copy the arguments of the if-operation's true- and false-block to the newly created loop-operation.
            for (auto& arg : ifOp->getTrueBlockInvocation().getArguments()) {
                loopOp->getLoopBodyBlock().addArgument(arg);
            }
            loopOp->getLoopFalseBlock().setBlock(ifOp->getFalseBlockInvocation().getBlock());
            for (auto& arg : ifOp->getFalseBlockInvocation().getArguments()) {
                loopOp->getLoopFalseBlock().addArgument(arg);
            }
            currentBlock->replaceTerminatorOperation(loopOp);
            // Next, we attempt to recognize whether the loop is a counted loop.
            // If the loop-header block has more than 2 incoming edges, we disregard it as a counted loop candidate.
            if (currentBlock->getNumLoopBackEdges() < 2 && currentBlock->getPredecessors().size() == 2) {
                // First, we check whether the comparison operation of the loop-header-if-operation allows us to
                // detect a counted loop.
                std::shared_ptr<Operations::CompareOperation> compareOp;
                bool comparisonContainsEqual = false;
                if (ifOp->getBooleanValue()->getOperationType() == Operations::Operation::OperationType::CompareOp) {
                    compareOp = std::static_pointer_cast<Operations::CompareOperation>(ifOp->getBooleanValue());
                }
                // Check if less or equal than (<,==,or) or greater or equal than (>,==,or) is given.
                // (<: ST(smaller than), >: GT(greater than)).
                if (ifOp->getBooleanValue()->getOperationType() == Operations::Operation::OperationType::OrOp) {
                    auto orOp = std::static_pointer_cast<Operations::OrOperation>(ifOp->getBooleanValue());
                    if (orOp->getLeftInput()->getOperationType() == Operations::Operation::OperationType::CompareOp
                        && orOp->getRightInput()->getOperationType() == Operations::Operation::OperationType::CompareOp) {
                        auto potentialEqualsOp = std::static_pointer_cast<Operations::CompareOperation>(orOp->getRightInput());
                        auto potentialSTorGTOp = std::static_pointer_cast<Operations::CompareOperation>(orOp->getLeftInput());
                        // A '<=' or '>=' operation is given, if the or operation has an '==' and a ('>' or '<')
                        // operation as left and right input, and if both input compare operations compare exactly
                        // the same values.
                        if (potentialEqualsOp->isEquals() && potentialSTorGTOp->isLessThanOrGreaterThan()
                            && potentialEqualsOp->getLeftInput() == potentialSTorGTOp->getLeftInput()
                            && potentialEqualsOp->getRightInput() == potentialSTorGTOp->getRightInput()) {
                            compareOp = std::move(potentialSTorGTOp);
                            comparisonContainsEqual = true;
                        }
                    }
                }
                if (compareOp) {
                    // We detected a valid compare operation, which contains the loop-induction-variable and the
                    // upperBound. However, we do cannot determine which is which yet.
                    // Thus, we first take the block that linked back to the loop-header the 'loopEndBlock'(priorBlock)
                    // and check whether it's second to last operation is a valid
                    // candidate for the loop-count-operation (the operation that increments the induction variable).
                    auto countOp = priorBlock->getOperations().at(priorBlock->getOperations().size() - 2);
                    if (compareOp->getComparator() != Operations::CompareOperation::EQ
                        && (countOp->getOperationType() == Operations::Operation::OperationType::AddOp)
                        && priorBlock->getTerminatorOp()->getOperationType() == Operations::Operation::OperationType::BranchOp
                        && std::static_pointer_cast<Operations::BranchOperation>(priorBlock->getTerminatorOp())
                                ->getNextBlockInvocation()
                                .getOperationArgIndex(countOp)
                            != -1
                        && !compareOp->getLeftInput()->getStamp()->isFloat()) {
                        // A loop-count-operation, contains the loop-induction-variable, and the step size as inputs.
                        // The result of the loop-count-operation is passed to the loop-header as the new value
                        // of the induction variable. This allows us to figure out which input to the compare-operation
                        // is the loop-induction-variable.
                        auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(priorBlock->getTerminatorOp());
                        auto inductionVarArgIndex = branchOp->getNextBlockInvocation().getOperationArgIndex(countOp);
                        auto loopInductionVarArg = currentBlock->getArguments().at(inductionVarArgIndex);
                        std::shared_ptr<Operations::ConstIntOperation> inductionVar;
                        std::shared_ptr<Operations::ConstIntOperation> upperBound;
                        // Get the block that appears before the loop-header-block in the control flow (from root).
                        // We only check loops with 2 predecessors, so the predecessor that is not the loopEndBlock is correct.
                        auto loopBeforeBlock =
                            (currentBlock->getPredecessors().at(0).lock()->getIdentifier() != priorBlock->getIdentifier())
                            ? currentBlock->getPredecessors().at(0).lock()
                            : currentBlock->getPredecessors().at(1).lock();
                        // Check whether the loop-iteration-variable and/or the upperBound are defined in the loopBeforeBlock.
                        auto compareOpConstants = getCompareOpConstants(currentBlock, loopBeforeBlock, compareOp);
                        // If we successfully found the loop-iteration-variable, and the upperBound, we assign them.
                        if (compareOpConstants.first && compareOpConstants.second) {
                            if (compareOp->getLeftInput() == loopInductionVarArg) {
                                if (compareOp->isLess()) {
                                    inductionVar = compareOpConstants.first;
                                    upperBound = compareOpConstants.second;
                                } else {
                                    NES_DEBUG(
                                        "Could not detect counted loop. The loop condition seems to lead to an infinite loop.");
                                    return;
                                }
                            } else if (compareOp->getRightInput() == loopInductionVarArg) {
                                if (compareOp->isGreater()) {
                                    inductionVar = compareOpConstants.second;
                                    upperBound = compareOpConstants.first;
                                } else {
                                    NES_DEBUG(
                                        "Could not detect counted loop. The loop condition seems to lead to an infinite loop.");
                                    return;
                                }
                            } else {
                                NES_DEBUG("Could not detect counted loop. The loop induction variable is not part of the "
                                          "loop-header comparison operation.");
                                return;
                            }
                        } else {
                            NES_DEBUG("Could not detect counted loop. Either the loop induction variable or the loop stop "
                                      "variable could not be detected.");
                            return;
                        }
                        // Get the stepSize from the loopEndBlock.
                        std::shared_ptr<Operations::ConstIntOperation> stepSize = getStepSize(priorBlock, countOp);
                        if (!stepSize) {
                            NES_DEBUG("Could not detect counted loop. The loop induction and/or the stepSize is/are manipulated "
                                      "prior to the loopEndBlock.");
                            return;
                        }
                        // Finally, we check whether the relation between the loop-induction-variable and the upperBound
                        // and the stepSize are valid. Then we assign the values to the countedLoopInfo.
                        auto countedLoopInfo = std::make_unique<Operations::CountedLoopInfo>();
                        if (inductionVar->getValue() < upperBound->getValue() && stepSize->getValue() > 0) {
                            countedLoopInfo->lowerBound = inductionVar->getValue();
                            countedLoopInfo->stepSize = stepSize->getValue();
                            countedLoopInfo->upperBound = upperBound->getValue() + comparisonContainsEqual;
                        } else {
                            NES_DEBUG("Could not detect a counted loop. Reason 1: UpperBound == LowerBound, Reason 2: "
                                      "Found increasing loop (loop induction variable > loop stop variable), but "
                                      "the step size is negative.");
                            return;
                        }
                        // countedLoopInfo->loopEndBlock = std::move(priorBlock);
                        loopOp->setLoopType(Operations::LoopOperation::LoopType::CountedLoop);
                        loopOp->setLoopInfo(std::move(countedLoopInfo));
                    } else {
                        NES_DEBUG("Could not detect counted loop. Possible reasons: \n"
                                  "1. The count-operation is not an addition operation.\n"
                                  "2. The loop-header comparison uses floating point types.\n"
                                  "3. The loop-end-block does not use a branch-operation to loop back\n"
                                  "4. The result of the count-operation is not an argument of the loop-header.\n"
                                  "5. The compare operation uses an equal comparator, which we do not support.\n");
                    }
                } else {
                    // loop-header does not use a comparison operation for boolean value.
                    NES_DEBUG("Loop header without comparison operation not supported. This currently includes '!='");
                }
            }
        }
    }
}

void LoopDetectionPhase::LoopDetectionPhaseContext::findLoopHeadBlocks(IR::BasicBlockPtr currentBlock) {
    std::stack<IR::BasicBlockPtr> ifBlocks;
    std::unordered_set<std::string> loopHeaderCandidates;
    std::unordered_set<std::string> visitedBlocks;
    IR::BasicBlockPtr priorBlock = currentBlock;

    bool returnBlockVisited = false;
    bool noMoreIfBlocks = true;
    // We iterate over the IR graph starting with currentBlock being the body of the root-operation.
    // We stop iterating when we have visited the return block at least once, and there are no more
    // unvisited if-blocks on the stack. If the IR graph is valid, no more unvisited if-operations exist.
    do {
        // Follow a branch through the query branch until currentBlock is either the return- or an already visited block.
        checkBranchForLoopHeadBlocks(currentBlock, ifBlocks, visitedBlocks, loopHeaderCandidates, priorBlock);
        // Set the current values for the loop halting values.
        noMoreIfBlocks = ifBlocks.empty();
        returnBlockVisited = returnBlockVisited
            || (currentBlock->getTerminatorOp()->getOperationType() == Operations::Operation::OperationType::ReturnOp);
        if (!noMoreIfBlocks) {
            priorBlock = ifBlocks.top();
            // When we take the false-branch of an ifOperation, we completely exhausted its true-branch.
            // Since loops can only loop back on their true-branch, we can safely stop tracking it as a loop candidate.
            loopHeaderCandidates.erase(ifBlocks.top()->getIdentifier());
            // Set currentBlock to first block in false-branch of ifOperation.
            // The false branch might contain nested loop-operations.
            if (ifBlocks.top()->getTerminatorOp()->getOperationType() == Operations::Operation::OperationType::IfOp) {
                currentBlock = std::static_pointer_cast<IR::Operations::IfOperation>(ifBlocks.top()->getTerminatorOp())
                                   ->getFalseBlockInvocation()
                                   .getBlock();
            } else {
                currentBlock = std::static_pointer_cast<IR::Operations::LoopOperation>(ifBlocks.top()->getTerminatorOp())
                                   ->getLoopFalseBlock()
                                   .getBlock();
            }
            ifBlocks.pop();
        }
    } while (!(noMoreIfBlocks && returnBlockVisited));
}
}//namespace NES::Nautilus::IR