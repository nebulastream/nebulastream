

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCKREF_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCKREF_HPP_

#include <Experimental/Interpreter/Trace/ValueRef.hpp>
#include <memory>
#include <vector>
namespace NES::Interpreter {
class ConstantValue;
class BlockRef {
  public:
    BlockRef(uint64_t block) : block(block){};
    uint64_t block;
    std::vector<ValueRef> arguments;
    friend std::ostream& operator<<(std::ostream& os, const ConstantValue& tag);
};
}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCKREF_HPP_
