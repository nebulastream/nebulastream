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

#include <Nautilus/IR/BasicBlocks/BasicBlockArgument.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
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
#include <optional>
#include <stack>
#include <string>
#include <unordered_map>

namespace NES::Nautilus::IR {

void LoopDetectionPhase::applyLoopDetection(std::shared_ptr<IRGraph> ir) {
    if(ir->getAppliedPhases() == IRGraph::AppliedPhases::RemoveBrOnlyBlocksPhase) {
        this->loopDetectionPhaseContext = std::make_unique<LoopDetectionPhaseContext>(std::move(ir));
        loopDetectionPhaseContext->processLoopDetection();
    } else {
        NES_ERROR("LoopDetectionPhase::applyLoopDetection: Could not apply LoopDetectionPhase. Requires IRGraph " <<
                    "to have BrOnlyBlocksPhase applied, but applied phase was: " << ir->printAppliedPhases());
    }
};
void LoopDetectionPhase::applyCountedLoopDetection() {
    if(loopDetectionPhaseContext)  {
        loopDetectionPhaseContext->processCountedLoopDetection();
    } else {
        NES_ERROR("LoopDetectionPhase::applyCountedLoopDetection: Cannot apply counted loop detection before " <<
                    "executing 'applyLoopDetection()'.");
    }
};

void LoopDetectionPhase::LoopDetectionPhaseContext::processLoopDetection() {
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    findLoopHeadBlocks(rootOperation->getFunctionBasicBlock());
    ir->setAppliedPhases(IRGraph::AppliedPhases::LoopDetectionPhase);
}
void LoopDetectionPhase::LoopDetectionPhaseContext::processCountedLoopDetection() {
    if(ir->getAppliedPhases() == IRGraph::AppliedPhases::ValueScopingPhase) {
        findCountedLoops();
        ir->setAppliedPhases(IRGraph::AppliedPhases::CountedLoopDetectionPhase);
    } else {
        NES_ERROR("LoopDetectionPhase::applyLoopDetection: Could not apply LoopDetectionPhase. Requires IRGraph " <<
                    "to have BrOnlyBlocksPhase applied, but applied phase was: " << ir->printAppliedPhases());
    }
}

void LoopDetectionPhase::LoopDetectionPhaseContext::checkBranchForLoopHeadBlocks(
        BasicBlockPtr& currentBlock, std::stack<BasicBlockPtr>& ifBlocks, 
        std::unordered_set<std::string>& visitedBlocks, std::unordered_set<std::string>& loopHeaderCandidates,
        BasicBlockPtr& priorBlock) {
    // Follow the true-branch of the current, and all nested if-operations until either
    // currentBlock is an already visited block, or currentBlock is the return-block.
    // Newly encountered if-operations are added as loopHeadCandidates.
    while(!visitedBlocks.contains(currentBlock->getIdentifier()) && 
            currentBlock->getTerminatorOp()->getOperationType() != Operations::Operation::ReturnOp) {
        // findAndAddConstantOperations(currentBlock, constantValues);
        auto terminatorOp = currentBlock->getTerminatorOp();
        if(terminatorOp->getOperationType() == Operations::Operation::BranchOp) {
            auto nextBlock = std::static_pointer_cast<Operations::BranchOperation>(
                terminatorOp)->getNextBlockInvocation().getNextBlock();
            visitedBlocks.emplace(currentBlock->getIdentifier());
            priorBlock = currentBlock;
            currentBlock = nextBlock;
        } else if (terminatorOp->getOperationType() == Operations::Operation::IfOp) {
            auto ifOp = std::static_pointer_cast<Operations::IfOperation>(terminatorOp);
            loopHeaderCandidates.emplace(currentBlock->getIdentifier());
            ifBlocks.emplace(currentBlock);
            visitedBlocks.emplace(currentBlock->getIdentifier());
            priorBlock = currentBlock;
            currentBlock = ifOp->getTrueBlockInvocation().getNextBlock();
        }
    }
    // If currentBlock is an already visited block that also is a loopHeaderCandidate, we found a loop-header-block.
    if(loopHeaderCandidates.contains(currentBlock->getIdentifier())) {
        currentBlock->incrementNumLoopBackEdge();
        // Loop header blocks always have an if-operation as their terminator operation.
        // But because we convert it to a loop-operation, the below condition is only true on the first visit.
        if(currentBlock->getTerminatorOp()->getOperationType() == Operations::Operation::IfOp) {
            // We convert this if-operation to a general loop operation.
            auto ifOp = std::static_pointer_cast<Operations::IfOperation>(currentBlock->getTerminatorOp());
            auto loopOp = std::make_shared<Operations::LoopOperation>(
                Operations::LoopOperation::LoopType::DefaultLoop, ifOp->getValue());
            loopOp->getLoopHeadBlock().setBlock(currentBlock);
            loopOp->getLoopBodyBlock().setBlock(ifOp->getTrueBlockInvocation().getNextBlock());
            loopOp->addLoopEndBlock(priorBlock);
            // Copy the arguments of the if-operation's true- and false-block to the newly created loop-operation.
            for(auto& arg: ifOp->getTrueBlockInvocation().getBranchOps()) {
                loopOp->getLoopBodyBlock().addArgument(arg);
            }
            loopOp->getLoopFalseBlock().setBlock(ifOp->getFalseBlockInvocation().getNextBlock());
            for(auto& arg: ifOp->getFalseBlockInvocation().getBranchOps()) {
                loopOp->getLoopFalseBlock().addArgument(arg);
            }
            loopOps.emplace_back(loopOp);
            currentBlock->replaceTerminatorOperation(loopOp);
        } else {
            // If terminator operation is not an IfOp, it must be a LoopOp
            NES_ASSERT(currentBlock->getTerminatorOp()->getOperationType() == Operations::Operation::LoopOp, 
                        "LoopDetectionPhase::LoopDetectionPhaseContext::checkBranchForLoopHeadBlocks: Terminator " <<
                         "operation of detected loop-header-block is neither if- nor loop-operation.");
            auto loopOp = std::static_pointer_cast<Operations::LoopOperation>(currentBlock->getTerminatorOp());
            auto duplicate = std::find(loopOp->getLoopEndBlocks().begin(), loopOp->getLoopEndBlocks().end(), priorBlock);
            NES_ASSERT(duplicate == loopOp->getLoopEndBlocks().end(), 
                            "LoopDetectionPhase::LoopDetectionPhaseContext::checkBranchForLoopHeadBlocks: " << 
                            "Tried to add the same block as LoopEndBlock to a loop operation twice.");
            loopOp->addLoopEndBlock(priorBlock);
        }
    }
}

void LoopDetectionPhase::LoopDetectionPhaseContext::findLoopHeadBlocks(BasicBlockPtr currentBlock) {
    std::stack<BasicBlockPtr> ifBlocks;
    std::unordered_set<std::string> loopHeaderCandidates;
    std::unordered_set<std::string> visitedBlocks;
    BasicBlockPtr priorBlock = currentBlock;

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
        returnBlockVisited = returnBlockVisited || 
            (currentBlock->getTerminatorOp()->getOperationType() == Operations::Operation::ReturnOp);
        if(!noMoreIfBlocks) {
            priorBlock = ifBlocks.top();
            // When we take the false-branch of an ifOperation, we completely exhausted its true-branch.
            // Since loops can only loop back on their true-branch, we can safely stop tracking it as a loop candidate.
            loopHeaderCandidates.erase(ifBlocks.top()->getIdentifier());
            // Set currentBlock to first block in false-branch of ifOperation.
            // The false branch might contain nested loop-operations.
            if(ifBlocks.top()->getTerminatorOp()->getOperationType() == Operations::Operation::IfOp) {
                currentBlock = std::static_pointer_cast<Operations::IfOperation>(ifBlocks.top()->getTerminatorOp())
                                ->getFalseBlockInvocation().getNextBlock();
            } else {
                currentBlock = std::static_pointer_cast<Operations::LoopOperation>(ifBlocks.top()->getTerminatorOp())
                                ->getLoopFalseBlock().getNextBlock();
            }
            ifBlocks.pop();
        }
    } while (!(noMoreIfBlocks && returnBlockVisited));
}

//==---------------------------------------------==//
//==----------- Counted Loop Detection ----------==//
//==---------------------------------------------==//
std::optional<OperationPtr> LoopDetectionPhase::LoopDetectionPhaseContext::getCompareOp(OperationPtr loopCondition) {
    std::shared_ptr<Operations::CompareOperation> compareOp;
    bool comparisonContainsEqual = false;
    if(loopCondition->getOperationType() == Operations::Operation::CompareOp) {
        return std::make_optional(std::static_pointer_cast<Operations::CompareOperation>(loopCondition));
    }
    // Check if less or equal than (<,==,or) or greater or equal than (>,==,or) is given.
    // (<: ST(smaller than), >: GT(greater than)).
    if(loopCondition->getOperationType () == Operations::Operation::OrOp) {
        auto orOp = std::static_pointer_cast<Operations::OrOperation>(loopCondition);
        if(orOp->getLeftInput()->getOperationType() == Operations::Operation::CompareOp && 
            orOp->getRightInput()->getOperationType() == Operations::Operation::CompareOp) {
            auto potentialEqualsOp = std::static_pointer_cast<Operations::CompareOperation>(orOp->getRightInput());
            auto potentialSTorGTOp = std::static_pointer_cast<Operations::CompareOperation>(orOp->getLeftInput());
            // A '<=' or '>=' operation is given, if the or operation has an '==' and a ('>' or '<')
            // operation as left and right input, and if both input compare operations compare exactly
            // the same values.
            if(potentialEqualsOp->isEquals() && potentialSTorGTOp->isLessThanOrGreaterThan()
                    && potentialEqualsOp->getLeftInput() == potentialSTorGTOp->getLeftInput()
                    && potentialEqualsOp->getRightInput() == potentialSTorGTOp->getRightInput()) {
                return std::make_optional(std::move(potentialSTorGTOp));
            }
        }
    }
    return std::nullopt;
}

std::string getDebugMessageString(std::shared_ptr<Operations::LoopOperation> loopOp) {
    return "ValueScopingPhase::ReplaceArguments: Could not detect Counted Loop in block: " + 
            loopOp->getLoopHeadBlock().getNextBlock()->getIdentifier() + ". Reason: ";
}

void LoopDetectionPhase::LoopDetectionPhaseContext::findCountedLoops() {
    for(auto& loopOp : loopOps) {
        if(loopOp->getLoopEndBlocks().size() > 1) {
            NES_DEBUG(getDebugMessageString(loopOp) << "Loop body has more than 1 final block (loopEndBlock).");
            continue;
        }
        std::shared_ptr<Operations::BasicBlockArgument> inductionVar;
        OperationPtr limitOp;
        OperationPtr stepSize;
        auto loopCondition = getCompareOp(loopOp->getLoopCondition());
        if(!loopCondition.has_value()) {
            NES_DEBUG(getDebugMessageString(loopOp) << "Could not detect the loop compare operation.");
        }
        auto conditionOps = loopCondition.value()->getInputs(loopOp->getLoopCondition());
        // Check one of the condition ops is constant throughout the loop-body (stepSize is required to be constant).
        if(conditionOps.at(0)->getOperationType() == Operations::Operation::BasicBlockArgument && 
           conditionOps.at(1)->getOperationType() == Operations::Operation::BasicBlockArgument) {
            auto leftArg = std::static_pointer_cast<Operations::BasicBlockArgument>(conditionOps.at(0));
            auto rightArg = std::static_pointer_cast<Operations::BasicBlockArgument>(conditionOps.at(1));
            // Check if the loop end block (always only 1 at this point) returns one of the arguments unchanged.
            int indexRightArg = loopOp->getLoopHeadBlock().getNextBlock()->getIndexOfArgument(rightArg);
            int indexLeftArg = loopOp->getLoopHeadBlock().getNextBlock()->getIndexOfArgument(leftArg);
            auto loopEndBlockInvocation = loopOp->getLoopEndBlocks().at(0)->getNextBlockInvocations().at(0);
            if(loopEndBlockInvocation->getBranchOps().at(indexRightArg) == rightArg) {
                limitOp = conditionOps.at(1);
                inductionVar = leftArg;
            } else if(loopEndBlockInvocation->getBranchOps().at(indexLeftArg) == leftArg) {
                limitOp = conditionOps.at(0);
                inductionVar = rightArg;
            } else {
                NES_DEBUG(getDebugMessageString(loopOp) << "Detected limit is not valid (multiple unique inputs).");
                continue;
            }
        } else {
            if(conditionOps.at(0)->getOperationType() != Operations::Operation::BasicBlockArgument) {
                limitOp = conditionOps.at(0);
                inductionVar = std::static_pointer_cast<Operations::BasicBlockArgument>(conditionOps.at(1));
            } else if(conditionOps.at(1)->getOperationType() != Operations::Operation::BasicBlockArgument) {
                limitOp = conditionOps.at(1);
                inductionVar = std::static_pointer_cast<Operations::BasicBlockArgument>(conditionOps.at(0));
            }
        }
        // check if both limitOp and inductionVar have a int or float stamp
        if(!(limitOp->getStamp()->isFloat() || limitOp->getStamp()->isInteger()) &&
           !(inductionVar->getStamp()->isFloat() || inductionVar->getStamp()->isInteger())) {
            NES_DEBUG(getDebugMessageString(loopOp) << "Detected limit or induction variable not of type int or float.");
            continue;
        }   //Todo: loopId should be the substring until '_' is met! if we have basic block 15717, the first two letters wont do
        auto loopId = loopOp->getLoopHeadBlock().getNextBlock()->getArguments().at(0)->getIdentifier().substr(0,2);
        // Find the first potential operation that increments the loop-induction-variable.
        // Todo we probably need to adapt this
        auto countOp = inductionVar->getBaseOperationPtr();
        // while(countOp->getOperationType() == Operations::Operation::BasicBlockArgument && countOp != inductionVar) {
        //     countOp = inductionVar->getBaseOps().back();
        // }
        // We only support positively increasing arithmetic operations as loop-count-ops.
        if(countOp != inductionVar && (countOp->getOperationType() == Operations::Operation::AddOp || 
                                           countOp->getOperationType() == Operations::Operation::MulOp) ) {
            // One of the inputs to the countOp must be the stepSize, the other is the loop-induction-variable.
            auto inputOps = countOp->getInputs(countOp);
            if(inputOps.size() == 2) {
                // If one of the inputs of the countOp is the loop-induction-variable, the other one must be the stepSize.
                if(inputOps.at(0) == inductionVar) {
                    stepSize = inputOps.at(1);
                    inputOps.erase(inputOps.begin());
                } else if(inputOps.at(1) == inductionVar) {
                    stepSize = inputOps.at(0);
                    inputOps.erase(inputOps.begin()+1);
                }
                // We must at least check whether stepSize qualifies as a valid stepSize for a count loop.
                // If neither inputs of the countOp are the loop-induction-variable, we must also check whether one of
                // the two inputs can be traced back to the loop-induction-variable on all upwards paths towards
                // the loop-header.
                for(auto& inputOp : inputOps) {
                    std::unordered_set<Operations::Operation*> trackedArgs;
                    std::stack<Operations::OperationPtr> argStack;
                    argStack.emplace(inputOp);
                    trackedArgs.emplace(inputOp.get());
                    bool isValidCountVariable = true;
                    bool isInductionVariable = false;
                    while(!argStack.empty()) {
                        auto currentOp = argStack.top();
                        argStack.pop();
                        if(currentOp->getOperationType() != Operations::Operation::BasicBlockArgument) {
                            // The loop-induction-var is guaranteed not to lead to a ConstOp, because a ConstOp 
                            // defined in the loop-body cannot be returned to the loop-header.
                            if(currentOp->getOperationType() == Operations::Operation::ConstFloatOp || 
                                currentOp->getOperationType() == Operations::Operation::ConstIntOp) {
                                // We found a stepSize candidate.
                                continue;
                            }
                            isValidCountVariable = false;
                            break;
                        }
                        // Check if currentArg is the loopInductionVariable. We only check up to the loop-header-block.
                        // If we never find a match with the inductionVar, we abort later.
                        auto currentArg = std::static_pointer_cast<Operations::BasicBlockArgument>(currentOp);
                        if(currentArg == inductionVar) {
                            isInductionVariable = true;
                            continue;
                        } else if (currentArg->getIdentifier().substr(0,2) == loopId) {
                            continue;
                        } else {
                            // Only add new base operations to the argStack.
                            // Todo replace
                            // for(auto& input : currentArg->getBaseOps()) {
                            //     if(!trackedArgs.contains(input.get())) {
                            //         argStack.emplace(input);
                            //         trackedArgs.emplace(input.get());
                            //     }
                            // }
                        }
                    }
                    if(isValidCountVariable) {
                        if(!isInductionVariable) {
                            stepSize = inputOp;
                        }
                    } else {
                        NES_DEBUG(getDebugMessageString(loopOp) << "loop-induction-variable or stepSize not valid.");
                        continue;
                    }
                }
            } else {
                NES_DEBUG(getDebugMessageString(loopOp) << "The detected increment op: " << countOp->toString() << 
                            ", does not have 2 inputs.");
                continue;
            }
        } else {
            NES_DEBUG(getDebugMessageString(loopOp) << "A suited increment operation could not be found.");
            continue;
        }
        // Make sure that stepSize is of type integer or float.
        if(!(stepSize->getStamp()->isFloat() || stepSize->getStamp()->isInteger())) {
            NES_DEBUG(getDebugMessageString(loopOp) << "The stepSize is not of type int or float.");
            continue;
        }
        // We detect loop-iteration-args and loop-yield-ops(for MLIR).
        std::vector<OperationPtr> iterationArgIndexes;
        std::vector<OperationPtr> yieldOps;
        // 'at(0)' is ok, because we assume one loopHeaderBlock with a branch operation.
        auto loopEndArgs = loopOp->getLoopEndBlocks().at(0)->getNextBlockInvocations().at(0)->getBranchOps();
        uint32_t argIndex = 0;
        // Todo need to check whether there are multiple unique base values.
        // -> why is it not sufficient to check whether the arg is different at the loopEndBlock
        // -> OF COURSE: because of loop ops in-between...
        // -> but then how to tell whether an arg was changed due to a loop, or it was actually changed?
        // -> currently, no 'easy' way -> would need to follow up the baseOperation ladder
        // -> if all prior 'versions' are BasicBlockArguments, then it is not an iterationArg
        // -> IDEA: pass on 'real' base values through loopEndBlocks
        //      -> if an arg was changed within the loop, or is a merge-arg, it does not get a 'real' base value
        //      -> if an arg remained the same, it gets a 'real' base value, which is the value that was inserted into
        //         the loop-header-block
        //  -> Question: Can this help making the above 'valid induction var and stepSize detection' easier?
        //      -> yes: we can simply check the 'real' base value and thereby skip the entire process completely
        for(auto& arg : loopOp->getLoopHeadBlock().getNextBlock()->getArguments()) {
            if(arg != inductionVar && arg != limitOp && arg != stepSize && 
               arg != loopEndArgs.at(argIndex)) {
                iterationArgIndexes.emplace_back(arg);
                auto testDebugArg = std::static_pointer_cast<Operations::BasicBlockArgument>(loopEndArgs.at(argIndex));
                yieldOps.emplace_back(loopEndArgs.at(argIndex));
            }
            ++argIndex;
        }
        // We found all information relevant to convert the loopOp to a countedLoopOp.
        auto countedLoopInfo = std::make_unique<Operations::CountedLoopInfo>(std::move(inductionVar), std::move(limitOp), 
            std::move(stepSize), std::move(iterationArgIndexes), std::move(yieldOps));
        loopOp->setLoopType(Operations::LoopOperation::LoopType::CountedLoop);
        loopOp->setLoopInfo(std::move(countedLoopInfo));
    }
}
}//namespace NES::Nautilus::IR