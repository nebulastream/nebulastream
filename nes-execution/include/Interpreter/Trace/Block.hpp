#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCK_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCK_HPP_
#include <Interpreter/Trace/Operation.hpp>
#include <cinttypes>
#include <ostream>
#include <unordered_map>
#include <vector>
namespace NES::Interpreter {
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

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCK_HPP_
