#include <Nautilus/Tracing/SymbolicExecution/TraceTerminationException.hpp>
#include <Nautilus/Tracing/Trace/OperationRef.hpp>
#include <Nautilus/Tracing/TraceContext2.hpp>

#include <Nautilus/Tracing/ValueRef.hpp>
#include <folly/SingletonThreadLocal.h>

namespace NES::Nautilus::Tracing {

static thread_local TraceContext2* traceContext;
TraceContext2* TraceContext2::get() { return traceContext; }

TraceContext2* TraceContext2::initialize() {
    traceContext = new TraceContext2();
    return traceContext;
}

TraceContext2::TraceContext2() : executionTrace(std::make_unique<ExecutionTrace>()) {
    this->symbolicExecutionContext = std::make_unique<SymbolicExecutionContext2>();
};

void TraceContext2::initializeTraceIteration(std::unique_ptr<TagRecorder> tagRecorder) {
    executionTrace->setCurrentBlock(0);
    currentOperationCounter = 0;
    executionTrace->tagMap.merge(executionTrace->localTagMap);
    executionTrace->localTagMap.clear();
    symbolicExecutionContext->next();
    this->tagRecorder = std::move(tagRecorder);
}

void TraceContext2::traceBinaryOperation(const OpCode& op,
                                         const ValueRef& leftRef,
                                         const ValueRef& rightRef,
                                         const ValueRef& resultRef) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(op)) {
        auto tag = tagRecorder->createTag();
        // NES_TRACE(magic_enum::enum_name<OpCode>(operation.op) << " tag: " << tag);
        if (op != ASSIGN) {
            if (auto ref = executionTrace->findKnownOperation(tag)) {
                if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                    auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                    auto mergeOperation = mergeBlock.operations.front();
                    currentOperationCounter = 1;
                    return;
                }
            }
        }
        auto operation = Nautilus::Tracing::TraceOperation(op, resultRef, {leftRef, rightRef});
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

void TraceContext2::traceReturnOperation(const ValueRef& resultRef) {
    if (!isExpectedOperation(RETURN)) {
        auto tag = tagRecorder->createTag();
        TraceOperation result = TraceOperation(RETURN);
        result.result = resultRef;
        executionTrace->addOperation(result);
        executionTrace->localTagMap.emplace(tag, result.operationRef);
    }
}

void TraceContext2::traceConstOperation(const ValueRef& ref, const AnyPtr& constantValue) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(CONST)) {
        auto tag = tagRecorder->createTag();
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

void TraceContext2::traceAssignmentOperation(const ValueRef& targetRef, const ValueRef& sourceRef) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    if (!isExpectedOperation(ASSIGN)) {
        auto tag = tagRecorder->createTag();
        auto operation = Nautilus::Tracing::TraceOperation(Nautilus::Tracing::ASSIGN, targetRef, {sourceRef});
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(tag, operation.operationRef);
    }
    incrementOperationCounter();
}

void TraceContext2::traceUnaryOperation(const OpCode&, const ValueRef&, const ValueRef&) {}

bool TraceContext2::traceCMP(const ValueRef& valueRef) {
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

    auto result = symbolicExecutionContext->executeCMP(this->tagRecorder.get());

    // set next block
    if (result) {
        executionTrace->setCurrentBlock(trueBlock);
    } else {
        executionTrace->setCurrentBlock(falseBlock);
    }
    currentOperationCounter = 0;
    return result;
}

ValueRef TraceContext2::createNextRef(NES::Nautilus::IR::Types::StampPtr type) {
    return ValueRef(executionTrace->getCurrentBlockIndex(), currentOperationCounter, type);
}

void TraceContext2::incrementOperationCounter() { currentOperationCounter++; }

bool TraceContext2::isExpectedOperation(const OpCode& opCode) {
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

std::shared_ptr<ExecutionTrace> TraceContext2::apply(const std::function<NES::Nautilus::Tracing::ValueRef()>& function) {
    auto tr = TagRecorder::createTagRecorder();
    while (symbolicExecutionContext->shouldContinue()) {
        try {
            initializeTraceIteration(std::make_unique<TagRecorder>(tr));
            auto result = function();
            traceReturnOperation(result);
        } catch (const TraceTerminationException& ex) {
        }
    }

    return executionTrace;
}

}// namespace NES::Nautilus::Tracing