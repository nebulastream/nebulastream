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

#include <Experimental/Interpreter/Util/Casting.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/Trace/OperationRef.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <execinfo.h>
#include <iostream>
#include <map>
namespace NES::Nautilus::Tracing {

static TraceContext* threadLocalTraceContext;
void initThreadLocalTraceContext() { threadLocalTraceContext = new TraceContext(); }
void disableThreadLocalTraceContext() {}
TraceContext* getThreadLocalTraceContext() { return threadLocalTraceContext; }

TraceContext::TraceContext() : executionTrace(std::make_unique<ExecutionTrace>()) {
    reset();
    startAddress = Tag::createCurrentAddress();
    std::cout << startAddress << std::endl;
}

void TraceContext::reset() {

    executionTrace->setCurrentBlock(0);
    currentOperationCounter = 0;
    executionTrace->tagMap.merge(executionTrace->localTagMap);
    executionTrace->localTagMap.clear();
};

ValueRef TraceContext::createNextRef(NES::Nautilus::IR::Types::StampPtr type) {
    return ValueRef(executionTrace->getCurrentBlockIndex(), currentOperationCounter, type);
}

std::shared_ptr<ExecutionTrace> TraceContext::getExecutionTrace() { return executionTrace; }

TraceOperation& TraceContext::getLastOperation() {
    auto& currentBlock = executionTrace->getCurrentBlock();
    return currentBlock.operations[currentOperationCounter];
}

bool TraceContext::isExpectedOperation(OpCode opCode) {
    auto& currentBlock = executionTrace->getCurrentBlock();
    if (currentBlock.operations.size() <= currentOperationCounter) {
        return false;
    }
    auto currentOperation = &currentBlock.operations[currentOperationCounter];
    // the next operation is a jump we transfer to that block.
    while (currentOperation->op == JMP) {
        executionTrace->setCurrentBlock(std::get<BlockRef>(currentOperation->input[0]).block);
        currentOperationCounter = 0;
        currentOperation = &executionTrace->getCurrentBlock().operations[currentOperationCounter];
    }
    return currentOperation->op == opCode;
}

void TraceContext::traceCMP(const ValueRef& valueRef, bool result) {
    uint32_t trueBlock;
    uint32_t falseBlock;
    if (!isExpectedOperation(CMP)) {
        trueBlock = executionTrace->createBlock();
        falseBlock = executionTrace->createBlock();
        executionTrace->getBlock(trueBlock).predecessors.emplace_back(executionTrace->getCurrentBlockIndex());
        executionTrace->getBlock(falseBlock).predecessors.emplace_back(executionTrace->getCurrentBlockIndex());
        auto operation = TraceOperation(CMP, valueRef, {BlockRef(trueBlock), BlockRef(falseBlock)});
        executionTrace->addOperation(operation);
    } else {
        // we repeat the operation
        auto operation = executionTrace->getCurrentBlock().operations[currentOperationCounter];
        trueBlock = std::get<BlockRef>(operation.input[0]).block;
        falseBlock = std::get<BlockRef>(operation.input[1]).block;
    }

    // set next block
    if (result) {
        executionTrace->setCurrentBlock(trueBlock);
    } else {
        executionTrace->setCurrentBlock(falseBlock);
    }
    currentOperationCounter = 0;
}

void TraceContext::trace(TraceOperation& operation) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(operation.op)) {
        auto tag = Tag::createTag(startAddress);
        std::cout << magic_enum::enum_name<OpCode>(operation.op) << " tag: " << tag << std::endl;
        if (operation.op != ASSIGN)
            if (auto ref = executionTrace->findKnownOperation(tag)) {
                if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                    auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                    auto mergeOperation = mergeBlock.operations.front();
                    currentOperationCounter = 1;
                    return;
                }
            }
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(std::make_pair(tag, operation.operationRef));
    }

    incrementOperationCounter();
}

void TraceContext::addTraceArgument(const ValueRef& value) { executionTrace->addArgument(value); }
void TraceContext::incrementOperationCounter() { currentOperationCounter++; }

}// namespace NES::Nautilus::Tracing