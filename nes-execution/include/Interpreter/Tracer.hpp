#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACER_HPP_
#include <memory>
#include <ostream>
#include <unordered_map>
#include <variant>
#include <vector>
#include <iostream>
namespace NES::Interpreter {

class Value;

class Tag {
  public:
    bool operator==(const Tag& other) const { return other.addresses == addresses; }
    friend std::ostream& operator<<(std::ostream& os, const Tag& tag);
    std::vector<uint64_t> addresses;
};

struct TagHasher {
    std::size_t operator()(const Tag& k) const {
        using std::hash;
        using std::size_t;
        using std::string;
        auto hasher = std::hash<uint64_t>();
        std::size_t hashVal = 1;
        for (auto address : k.addresses) {
            hashVal = hashVal ^ hasher(address) << 1;
        }
        return hashVal;
    }
};

enum OpCode { ADD, SUB, DIV, MUL, EQUALS, LESS_THAN, NEGATE, AND, OR, CMP, JMP, CONST };

class ValueRef {
  public:
    ValueRef() : blockId(), operationId(){};
    ValueRef(uint32_t blockId, uint32_t operationId) : blockId(blockId), operationId(operationId){};
    ValueRef(const ValueRef& other) : blockId(other.blockId), operationId(other.operationId) {}
    ValueRef& operator=(const ValueRef& other) {
        this->operationId = other.operationId;
        this->blockId = other.blockId;
        return *this;
    }
    uint32_t blockId;
    uint32_t operationId;
    bool operator==(const ValueRef& rhs) const;
    bool operator!=(const ValueRef& rhs) const;
    friend std::ostream& operator<<(std::ostream& os, const ValueRef& tag);
};

struct ValueRefHasher {
    std::size_t operator()(const ValueRef& k) const {
        auto hasher = std::hash<uint64_t>();
        std::size_t hashVal = hasher(k.operationId) ^ hasher(k.blockId);
        return hashVal;
    }
};

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

class Operation {
  public:
    Operation(OpCode op, ValueRef result, std::vector<InputVariant> input) : op(op), result(result), input(input){};
    Operation(const Operation& other) : op(other.op), result(other.result), input(other.input) {}
    ~Operation() {}
    OpCode op;
    InputVariant result;
    std::vector<InputVariant> input;
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
    void addOperation(Operation operation) {
        if (blocks.empty()) {
            createBlock();
        }
        blocks[currentBlock].operations.emplace_back(operation);
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
    ValueRef findReference(ValueRef ref, const ValueRef value);
    void checkInputReference(uint32_t currentBlock, ValueRef inputReference, ValueRef currentInput);
    ValueRef createBlockArgument(uint32_t blockIndex, ValueRef ref, ValueRef value);
    Block& processControlFlowMerge(uint32_t blockIndex, uint32_t operationIndex);
    friend std::ostream& operator<<(std::ostream& os, const ExecutionTrace& tag);

    std::unordered_map<Tag, std::tuple<uint32_t, uint32_t>, TagHasher> tagMap;
    std::unordered_map<Tag, std::tuple<uint32_t, uint32_t>, TagHasher> localTagMap;

    void traceAssignment(ValueRef value, ValueRef value1);

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
    void trace(OpCode op, const Value& left, const Value& right, Value& result);
    void trace(OpCode op, const Value& input, Value& result);

    ExecutionTrace& getExecutionTrace() { return executionTrace; };
    ValueRef createNextRef() { return ValueRef(executionTrace.getCurrentBlockIndex(), currentOperationCounter); }

  private:
    Tag createTag();
    uint64_t createStartAddress();
    ExecutionTrace executionTrace;
    uint64_t currentOperationCounter = 0;
    uint64_t startAddress;
};

inline void Trace(TraceContext* ctx, OpCode type, const Value& left, const Value& right, Value& result) {
    if (ctx != nullptr) {
        ctx->trace(type, left, right, result);
    }
}

inline void Trace(TraceContext* ctx, OpCode type, const Value& input, Value& result) {
    if (ctx != nullptr) {
        ctx->trace(type, input, result);
    }
}

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACER_HPP_
