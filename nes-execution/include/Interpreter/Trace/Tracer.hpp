#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACER_HPP_
#include <functional>
#include <iostream>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <variant>
#include <vector>
#include <Interpreter/Trace/Tag.hpp>
#include <Interpreter/Trace/ValueRef.hpp>
namespace NES::Interpreter {

class ConstantValue {
  public:
    ConstantValue(AnyPtr anyPtr) : value(std::move(anyPtr)){};
    AnyPtr value;
    friend std::ostream& operator<<(std::ostream& os, const ConstantValue& tag);
};

class BlockRef {
  public:
    BlockRef(uint64_t block) : block(block){};
    uint64_t block;
    std::vector<ValueRef> arguments;
    friend std::ostream& operator<<(std::ostream& os, const ConstantValue& tag);
};

using InputVariant = std::variant<ValueRef, ConstantValue, BlockRef>;

class OperationRef {
  public:
    OperationRef(uint32_t blockId, uint32_t operationId) : blockId(blockId), operationId(operationId) {}
    uint32_t blockId;
    uint32_t operationId;
};

class Operation {
  public:
    Operation(OpCode op, ValueRef result, std::vector<InputVariant> input) : op(op), result(result), input(input){};
    Operation(OpCode op) : op(op), result(), input(){};
    Operation(const Operation& other)
        : op(other.op), result(other.result), input(other.input), operationRef(other.operationRef) {}
    ~Operation() {}
    OpCode op;
    InputVariant result;
    std::vector<InputVariant> input;
    std::shared_ptr<OperationRef> operationRef;
    friend std::ostream& operator<<(std::ostream& os, const Operation& operation);
};

class Block {
  public:
    Block(uint32_t blockId) : blockId(blockId), frame(){};
    uint32_t blockId;
    // sequential list of operations
    std::vector<Operation> operations;
    std::vector<ValueRef> arguments;
    std::vector<uint32_t> predecessors;
    std::unordered_map<ValueRef, ValueRef, ValueRefHasher> frame;
    /**
     * @brief Check if this block defines a specific ValueRef as a parameter or result of an operation
     * @param ref
     * @return
     */
    bool isLocalValueRef(ValueRef& ref);
    void addArgument(ValueRef ref);
    friend std::ostream& operator<<(std::ostream& os, const Block& block);
};

class ExecutionTrace {
  public:
    ExecutionTrace() : tagMap(), blocks() { createBlock(); };
    ~ExecutionTrace(){};
    void addOperation(Operation& operation) {
        if (blocks.empty()) {
            createBlock();
        }
        operation.operationRef = std::make_shared<OperationRef>(currentBlock, blocks[currentBlock].operations.size());
        blocks[currentBlock].operations.emplace_back(operation);
        if (operation.op == RETURN) {
            returnRef = operation.operationRef;
        }
    }

    uint32_t createBlock() {
        blocks.emplace_back(blocks.size());
        return blocks.size() - 1;
    }

    Block& getBlock(uint32_t blockIndex) { return blocks[blockIndex]; }
    std::vector<Block>& getBlocks() { return blocks; }
    Block& getCurrentBlock() { return blocks[currentBlock]; }
    uint32_t getCurrentBlockIndex() { return currentBlock; }

    bool hasNextOperation() { return currentBlock; }

    void setCurrentBloc(uint32_t index) { currentBlock = index; }
    void traceCMP(Operation& operation);
    ValueRef findReference(ValueRef ref, const ValueRef value);
    void checkInputReference(uint32_t currentBlock, ValueRef inputReference, ValueRef currentInput);
    ValueRef createBlockArgument(uint32_t blockIndex, ValueRef ref, ValueRef value);
    Block& processControlFlowMerge(uint32_t blockIndex, uint32_t operationIndex);
    friend std::ostream& operator<<(std::ostream& os, const ExecutionTrace& tag);

    std::unordered_map<Tag, std::shared_ptr<OperationRef>, TagHasher> tagMap;
    std::unordered_map<Tag, std::shared_ptr<OperationRef>, TagHasher> localTagMap;

    void traceAssignment(ValueRef value, ValueRef value1);
    std::shared_ptr<OperationRef> returnRef;

  private:
    uint32_t currentBlock;
    std::vector<Block> blocks;
};

class TraceContext {
  public:
    TraceContext() {
        reset();
        startAddress = (uint64_t) (__builtin_return_address(1));
        std::cout << startAddress << std::endl;
    }
    void reset() {
        executionTrace.setCurrentBloc(0);
        currentOperationCounter = 0;
        executionTrace.tagMap.merge(executionTrace.localTagMap);
        executionTrace.localTagMap.clear();
    };
   // void trace(OpCode op, const Value& left, const Value& right, Value& result);
   // void trace(OpCode op, const Value& input, Value& result);
    void trace(Operation& operation);
    void traceCMP(const ValueRef& valueRef, bool result);
    bool isExpectedOperation(OpCode operation);
    std::shared_ptr<OperationRef> isKnownOperation(Tag& tag);

    ExecutionTrace& getExecutionTrace() { return executionTrace; };
    ValueRef createNextRef() { return ValueRef(executionTrace.getCurrentBlockIndex(), currentOperationCounter); }

  private:
    Tag createTag();
    uint64_t createStartAddress();
    ExecutionTrace executionTrace;
    uint64_t currentOperationCounter = 0;
    uint64_t startAddress;
};

template<typename Functor>
ExecutionTrace traceFunction(Functor func) {
    auto tracer = TraceContext();
    func(&tracer);
    Operation result = Operation(RETURN);
    tracer.trace(result);
    return tracer.getExecutionTrace();
}
/**
void Trace(OpCode op, const Value& left, const Value& right, Value& result) {
    if (ctx != nullptr) {
        auto operation = Operation(op, result.ref, {left.ref, right.ref});
        ctx->trace(operation);
    }
}

void Trace(OpCode op, const Value& input, Value& result) {
    if (ctx != nullptr) {
        if (op == OpCode::CONST) {
            auto constValue = input.value->copy();
            auto operation = Operation(op, result.ref, {ConstantValue(std::move(constValue))});
            ctx->trace(operation);
        } else if (op == CMP) {
            ctx->traceCMP(input.ref, cast<Boolean>(result.value)->value);
        } else {
            auto operation = Operation(op, result.ref, {input.ref});
            ctx->trace(operation);
        }
    }
}
 */

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACER_HPP_
