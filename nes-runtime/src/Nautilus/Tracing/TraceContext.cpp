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
#include <Nautilus/Tracing/SymbolicExecution/TraceTerminationException.hpp>
#include <Nautilus/Tracing/Trace/OperationRef.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Tracing {

static thread_local TraceContext* traceContext;
TraceContext* TraceContext::get() { return traceContext; }

TraceContext* TraceContext::initialize(TagRecorder& tagRecorder) {
    traceContext = new TraceContext(tagRecorder);
    return traceContext;
}

void TraceContext::terminate() {
    delete traceContext;
    traceContext = nullptr;
}

TraceContext::TraceContext(TagRecorder& tagRecorder)
    : tagRecorder(tagRecorder), executionTrace(std::make_unique<ExecutionTrace>()), symbolicExecutionContext(){};

void TraceContext::initializeTraceIteration() {
    executionTrace->setCurrentBlock(0);
    currentOperationCounter = 0;
    executionTrace->tagMap.merge(executionTrace->localTagMap);
    executionTrace->localTagMap.clear();
    symbolicExecutionContext.next();
}

void TraceContext::traceBinaryOperation(const OpCode& op,
                                        const ValueRef& leftRef,
                                        const ValueRef& rightRef,
                                        const ValueRef& resultRef) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(op)) {
        auto tag = tagRecorder.createTag();
        if (auto ref = executionTrace->findKnownOperation(tag)) {
            if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                auto mergeOperation = mergeBlock.operations.front();
                currentOperationCounter = 1;
                return;
            }
        }
        auto operation = Nautilus::Tracing::TraceOperation(op, resultRef, {leftRef, rightRef});
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

void TraceContext::traceReturnOperation(const ValueRef& resultRef) {
    if (!isExpectedOperation(RETURN)) {
        auto tag = tagRecorder.createTag();
        if (auto ref = executionTrace->findKnownOperation(tag)) {
            if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                auto mergeOperation = mergeBlock.operations.front();
                currentOperationCounter = 1;
                return;
            }
        }
        TraceOperation result = TraceOperation(RETURN);
        if (!resultRef.type->isVoid()) {
            result.input.emplace_back(resultRef);
            result.result = resultRef;
        } else {
            result.result = resultRef;
        }
        executionTrace->addOperation(result);
        executionTrace->localTagMap.emplace(tag, result.operationRef);
    }
}

void TraceContext::traceConstOperation(const ValueRef& ref, const AnyPtr& constantValue) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(CONST)) {
        auto tag = tagRecorder.createTag();
        if (auto ref = executionTrace->findKnownOperation(tag)) {
            if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                auto mergeOperation = mergeBlock.operations.front();
                currentOperationCounter = 1;
                return;
            }
        }

        auto operation = Nautilus::Tracing::TraceOperation(CONST, ref, {constantValue});
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

void TraceContext::traceFunctionCall(const std::vector<Nautilus::Tracing::InputVariant>& arguments) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(CALL)) {
        auto tag = tagRecorder.createTag();
        if (auto ref = executionTrace->findKnownOperation(tag)) {
            if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                auto mergeOperation = mergeBlock.operations.front();
                currentOperationCounter = 1;
                return;
            }
        }

        auto operation = Nautilus::Tracing::TraceOperation(CALL, arguments);
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

void TraceContext::traceStore(const ValueRef& memref, const ValueRef& valueRef) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(STORE)) {
        auto tag = tagRecorder.createTag();
        if (auto ref = executionTrace->findKnownOperation(tag)) {
            if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                auto mergeOperation = mergeBlock.operations.front();
                currentOperationCounter = 1;
                return;
            }
        }

        auto operation = Nautilus::Tracing::TraceOperation(STORE, {memref, valueRef});
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

void TraceContext::traceFunctionCall(const ValueRef& resultRef, const std::vector<Nautilus::Tracing::InputVariant>& arguments) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(CALL)) {
        auto tag = tagRecorder.createTag();
        if (auto ref = executionTrace->findKnownOperation(tag)) {
            if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                auto mergeOperation = mergeBlock.operations.front();
                currentOperationCounter = 1;
                return;
            }
        }

        auto operation = Nautilus::Tracing::TraceOperation(CALL, resultRef, arguments);
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

void TraceContext::traceAssignmentOperation(const ValueRef& targetRef, const ValueRef& sourceRef) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(ASSIGN)) {
        auto tag = tagRecorder.createTag();
        auto operation = Nautilus::Tracing::TraceOperation(Nautilus::Tracing::ASSIGN, targetRef, {sourceRef});
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

void TraceContext::addTraceArgument(const ValueRef& value) { executionTrace->addArgument(value); }

void TraceContext::traceUnaryOperation(const OpCode& op, const ValueRef& input, const ValueRef& result) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(op)) {
        auto tag = tagRecorder.createTag();
        if (auto ref = executionTrace->findKnownOperation(tag)) {
            if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                auto mergeOperation = mergeBlock.operations.front();
                currentOperationCounter = 1;
                return;
            }
        }

        auto operation = Nautilus::Tracing::TraceOperation(op, result, {input});
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

bool TraceContext::traceCMP(const ValueRef& valueRef) {
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

    auto result = symbolicExecutionContext.executeCMP(this->tagRecorder);

    // set next block
    if (result) {
        executionTrace->setCurrentBlock(trueBlock);
    } else {
        executionTrace->setCurrentBlock(falseBlock);
    }
    currentOperationCounter = 0;
    return result;
}

ValueRef TraceContext::createNextRef(NES::Nautilus::IR::Types::StampPtr type) {
    return ValueRef(executionTrace->getCurrentBlockIndex(), currentOperationCounter, type);
}

void TraceContext::incrementOperationCounter() { currentOperationCounter++; }

bool TraceContext::isExpectedOperation(const OpCode& opCode) {
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

std::shared_ptr<ExecutionTrace> TraceContext::apply(const std::function<NES::Nautilus::Tracing::ValueRef()>& function) {
    while (symbolicExecutionContext.shouldContinue()) {
        try {
            initializeTraceIteration();
            auto result = function();
            traceReturnOperation(result);
        } catch (const TraceTerminationException& ex) {
        }
    }
    NES_DEBUG("Iterations:" << symbolicExecutionContext.getIterations());
    return executionTrace;
}

}// namespace NES::Nautilus::Tracing