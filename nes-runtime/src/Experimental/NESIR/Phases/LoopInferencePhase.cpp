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
#include <Experimental/IR/Phases/LoopInferencePhase.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopInfo.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::IR {

std::shared_ptr<IRGraph> LoopInferencePhase::apply(std::shared_ptr<IRGraph> ir) {
    auto context = Context(ir);
    return context.process();
}

LoopInferencePhase::Context::Context(std::shared_ptr<IRGraph> ir) : ir(ir) {}

std::shared_ptr<IRGraph> LoopInferencePhase::Context::process() {
    auto rootBasicBlock = ir->getRootOperation()->getFunctionBasicBlock();
    processBlock(rootBasicBlock);
    return ir;
}

void LoopInferencePhase::Context::processBlock(BasicBlockPtr block) {

    if (block->getTerminatorOp()->getOperationType() == Operations::Operation::LoopOp) {
        // found loop node
        processLoop(block);
    }
    // check sub blocks
}

void LoopInferencePhase::Context::processLoop(BasicBlockPtr block) {
    auto optionalCountedLoopInfo = isCountedLoop(block);
    if (optionalCountedLoopInfo.has_value()) {
        return;
    }
}

std::optional<std::shared_ptr<Operations::CountedLoopInfo>>
LoopInferencePhase::Context::isCountedLoop(BasicBlockPtr preLoopBlock) {

    auto loopOperation = std::static_pointer_cast<Operations::LoopOperation>(preLoopBlock->getTerminatorOp());

    auto loopHeadBlock = loopOperation->getLoopHeadBlock().getBlock();
    // a head block in a counted loop should terminate with an if operation
    if (loopHeadBlock->getTerminatorOp()->getOperationType() != Operations::Operation::IfOp) {
        return nullptr;
    }

    auto ifOperation = std::static_pointer_cast<Operations::IfOperation>(loopHeadBlock->getTerminatorOp());
    if (ifOperation->getValue()->getOperationType() != Operations::Operation::CompareOp) {
        return nullptr;
    }
    // the comparator
    auto compareOperation = std::static_pointer_cast<Operations::CompareOperation>(ifOperation->getValue());
    if (compareOperation->getComparator() != Operations::CompareOperation::ISLT) {
        return nullptr;
    }

    auto upperBound = compareOperation->getRightInput();
    if (upperBound->getOperationType() != Operations::Operation::ConstIntOp
        && upperBound->getOperationType() != Operations::Operation::BasicBlockArgument) {
        return nullptr;
    }

    // In the following we try to detect the induction variable in the loop
    auto inductionVariableInLoopHead = compareOperation->getLeftInput();
    if (inductionVariableInLoopHead->getOperationType() != Operations::Operation::BasicBlockArgument) {
        return nullptr;
    }

    // get induction variable in pre loop block
    auto indexOfInductionVariable = loopHeadBlock->getIndexOfArgument(inductionVariableInLoopHead);
    // the induction variable in the pre loop is also the lower loop bound
    auto lowerLoopBound = loopOperation->getLoopHeadBlock().getArguments()[indexOfInductionVariable];

    // get loop body
    auto loopBody = ifOperation->getTrueBlockInvocation();
    auto loopBodyBlock = loopBody.getBlock();
    // currently we assume that counted-loops only have a single body block ->
    // thus the body block must have a branch that jumps back to the loop head.
    // TODO add support for multi block loop bodies
    if (loopBodyBlock->getTerminatorOp()->getOperationType() != Operations::Operation::BranchOp) {
        return nullptr;
    }

    auto loopBodyBranch = std::static_pointer_cast<Operations::BranchOperation>(loopBodyBlock->getTerminatorOp());
    // check if loop body branch jumps back to the loop head
    if (loopBodyBranch->getNextBlockInvocation().getBlock() != loopHeadBlock) {
        return nullptr;
    }

    // check if the induction variable in the loop body was modified by an add
    auto inductionVariableInLoopBody = loopBodyBranch->getNextBlockInvocation().getArguments()[indexOfInductionVariable];
    if (inductionVariableInLoopBody->getOperationType() != Operations::Operation::AddOp) {
        return nullptr;
    }
    auto loopBodyAddOperation = std::static_pointer_cast<Operations::AddOperation>(inductionVariableInLoopBody);
    // currently we assume that the left side of the add is a block parameter and that right side of the add is the constant step size.
    // todo extend this to also support non constant step sizes
    if (loopBodyAddOperation->getLeftInput()->getOperationType() != Operations::Operation::BasicBlockArgument) {
        return nullptr;
    }
    if (loopBodyAddOperation->getRightInput()->getOperationType() != Operations::Operation::ConstIntOp) {
        return nullptr;
    }
    auto stepSize = loopBodyAddOperation->getRightInput();

    // the loop is a counted loop, in the following we modify the blocks
    // 1. remove the modification of the induction variable in the loop body
    loopBodyBlock->removeOperation(stepSize);
    loopBodyBlock->removeOperation(loopBodyAddOperation);
    // remove induction variable from body parameters
    loopBodyBranch->getNextBlockInvocation().removeArgument(indexOfInductionVariable);

    // if the step size is a const int it is defined in the loop body -> thus we have to copy it to the loop pre head
    if (stepSize->getOperationType() == Operations::Operation::ConstIntOp) {
        preLoopBlock->addOperationBefore(loopOperation, stepSize);
    }

    // if the upper bound is a const int it is defined in the loop head -> thus we have to copy it to the loop pre head
    if (upperBound->getOperationType() == Operations::Operation::ConstIntOp) {
        preLoopBlock->addOperationBefore(loopOperation, upperBound);
    }

    auto countedLoopInfo = std::make_shared<Operations::CountedLoopInfo>();
    countedLoopInfo->upperBound = upperBound;
    countedLoopInfo->stepSize = stepSize;
    countedLoopInfo->lowerBound = lowerLoopBound;
    countedLoopInfo->loopBodyBlock = loopBodyBlock;
    countedLoopInfo->loopEndBlock = ifOperation->getFalseBlockInvocation().getBlock();
    countedLoopInfo->loopBodyInductionVariable = loopBodyAddOperation->getLeftInput();

    NES_DEBUG("Found counted loop" << lowerLoopBound << "-" << upperBound << "-" << stepSize);

    // copy iterator arguments
    for (auto argument : loopBodyBlock->getArguments()) {
        if (argument != loopBodyAddOperation->getLeftInput()) {
            countedLoopInfo->loopBodyIteratorArguments.emplace_back(argument);
        }
    }

    auto loopHeadBlockArguments = loopOperation->getLoopHeadBlock().getArguments();
    for (uint64_t i = 0; i < loopHeadBlockArguments.size(); i++) {
        if (i != indexOfInductionVariable) {
            countedLoopInfo->loopInitialIteratorArguments.emplace_back(loopHeadBlockArguments[i]);
        }
    }
    loopOperation->setLoopInfo(countedLoopInfo);
    return countedLoopInfo;
}

}// namespace NES::Nautilus::IR