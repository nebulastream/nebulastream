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
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/Trace/OperationRef.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Tracing {

/**
 * Create thread_local variable to store the trace context.
 * The trace context can always be thread local, as tracable code can never span multiple threads.
 */
static thread_local TraceContext* traceContext;

TraceContext* TraceContext::get() { return traceContext; }
TraceContext* TraceContext::getIfActive() { return shouldTrace() ? get() : nullptr; };

bool TraceContext::shouldTrace() { return get() != nullptr && get()->active; }

TraceContext* TraceContext::initialize(TagRecorder& tagRecorder) {
    traceContext = new TraceContext(tagRecorder);
    return traceContext;
}

void TraceContext::terminate() {
    delete traceContext;
    traceContext = nullptr;
}

TraceContext::TraceContext(TagRecorder& tagRecorder)
    : tagRecorder(tagRecorder), executionTrace(std::make_unique<ExecutionTrace>()), symbolicExecutionContext() {}

void TraceContext::initializeTraceIteration() {
    executionTrace->setCurrentBlock(0);
    currentOperationCounter = 0;
    tagMap.merge(localTagMap);
    localTagMap.clear();
    symbolicExecutionContext.next();
}

void TraceContext::traceBinaryOperation(const OpCode& op,
                                        const ValueRef& leftRef,
                                        const ValueRef& rightRef,
                                        const ValueRef& resultRef) {
    trace(op, [&]() {
        return Nautilus::Tracing::TraceOperation(op, resultRef, {leftRef, rightRef});
    });
}

void TraceContext::traceUnaryOperation(const OpCode& op, const ValueRef& input, const ValueRef& result) {
    trace(op, [&]() {
        return Nautilus::Tracing::TraceOperation(op, result, {input});
    });
}

void TraceContext::traceReturnOperation(const ValueRef& resultRef) {
    trace(OpCode::RETURN, [&]() {
        TraceOperation result = TraceOperation(OpCode::RETURN);
        if (!resultRef.type->isVoid()) {
            result.input.emplace_back(resultRef);
        }
        result.result = resultRef;
        return result;
    });
}

void TraceContext::traceConstOperation(const ValueRef& constRef, const AnyPtr& constantValue) {
    trace(OpCode::CONST, [&]() {
        return Nautilus::Tracing::TraceOperation(OpCode::CONST, constRef, {constantValue});
    });
}

void TraceContext::traceFunctionCall(const std::vector<Nautilus::Tracing::InputVariant>& arguments) {
    trace(OpCode::CALL, [&]() {
        return Nautilus::Tracing::TraceOperation(OpCode::CALL, arguments);
    });
}

void TraceContext::traceFunctionCall(const ValueRef& resultRef, const std::vector<Nautilus::Tracing::InputVariant>& arguments) {
    trace(OpCode::CALL, [&]() {
        return Nautilus::Tracing::TraceOperation(OpCode::CALL, resultRef, arguments);
    });
}

void TraceContext::traceStore(const ValueRef& memref, const ValueRef& valueRef) {
    trace(OpCode::STORE, [&]() {
        return Nautilus::Tracing::TraceOperation(OpCode::STORE, {memref, valueRef});
    });
}

void TraceContext::traceAssignmentOperation(const ValueRef& targetRef, const ValueRef& sourceRef) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(OpCode::ASSIGN)) {
        auto tag = tagRecorder.createTag();
        auto operation = Nautilus::Tracing::TraceOperation(Nautilus::Tracing::OpCode::ASSIGN, targetRef, {sourceRef});
        executionTrace->addOperation(operation);
        localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

void TraceContext::addTraceArgument(const ValueRef& value) { executionTrace->addArgument(value); }

template<typename Functor>
void TraceContext::trace(const OpCode& opCode, Functor createOperation) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(opCode)) {
        auto tag = tagRecorder.createTag();
        if (!isKnownOperation(tag)) {
            auto operation = createOperation();
            executionTrace->addOperation(operation);
            localTagMap.emplace(tag, operation.operationRef);
            incrementOperationCounter();
        }
    } else {
        incrementOperationCounter();
    }
}

bool TraceContext::traceCMP(const ValueRef& valueRef) {
    uint32_t trueBlock;
    uint32_t falseBlock;
    if (!isExpectedOperation(OpCode::CMP)) {
        trueBlock = executionTrace->createBlock();
        falseBlock = executionTrace->createBlock();
        executionTrace->getBlock(trueBlock).predecessors.emplace_back(executionTrace->getCurrentBlockIndex());
        executionTrace->getBlock(falseBlock).predecessors.emplace_back(executionTrace->getCurrentBlockIndex());
        auto operation = TraceOperation(OpCode::CMP, valueRef, {BlockRef(trueBlock), BlockRef(falseBlock)});
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

ValueRef TraceContext::createNextRef(const NES::Nautilus::IR::Types::StampPtr& type) {
    // If the next operation already exists, we have to create the same value ref.
    // Currently, we assume that it is correctly set.
    // TODO this should be handled better at a fundamental level, which ensures that value references are always correct.
    auto& currentBlock = executionTrace->getCurrentBlock();
    if (currentBlock.operations.size() > currentOperationCounter) {
        auto& operation = currentBlock.operations[currentOperationCounter];
        if (auto* valRef = std::get_if<ValueRef>(&operation.result)) {
            return *valRef;
        }
    }
    return {executionTrace->getCurrentBlockIndex(), currentOperationCounter, type};
}

void TraceContext::incrementOperationCounter() { currentOperationCounter++; }

bool TraceContext::isExpectedOperation(const OpCode& opCode) {
    auto& currentBlock = executionTrace->getCurrentBlock();
    if (currentBlock.operations.size() <= currentOperationCounter) {
        return false;
    }
    auto currentOperation = &currentBlock.operations[currentOperationCounter];
    // the next operation is a jump we transfer to that block.
    while (currentOperation->op == OpCode::JMP) {
        executionTrace->setCurrentBlock(std::get<BlockRef>(currentOperation->input[0]).block);
        currentOperationCounter = 0;
        currentOperation = &executionTrace->getCurrentBlock().operations[currentOperationCounter];
    }
    return currentOperation->op == opCode;
}

bool TraceContext::isKnownOperation(const Tag* tag) {
    if (auto ref = checkTag(tag)) {
        std::stringstream trace;
        trace << *executionTrace;
        NES_TRACE("{}", trace.str());
        // TODO #3500 Fix handling of repeated operations
        if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
            auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
            auto mergeOperation = mergeBlock.operations.front();
            currentOperationCounter = 1;
            return true;
        }
    }
    return false;
}

std::shared_ptr<OperationRef> TraceContext::checkTag(const Tag* tag) {
    if (tagMap.contains(tag)) {
        return tagMap.find(tag)->second;
    } else if (localTagMap.contains(tag)) {
        return localTagMap.find(tag)->second;
    }
    return nullptr;
}

std::shared_ptr<ExecutionTrace> TraceContext::apply(const std::function<NES::Nautilus::Tracing::ValueRef()>& function) {
    while (symbolicExecutionContext.shouldContinue()) {
        try {
            initializeTraceIteration();
            TraceContext::get()->resume();
            auto result = function();
            traceReturnOperation(result);
        } catch (const TraceTerminationException& ex) {
        }
    }
    NES_DEBUG("Iterations: {}", symbolicExecutionContext.getIterations());
    return executionTrace;
}

}// namespace NES::Nautilus::Tracing