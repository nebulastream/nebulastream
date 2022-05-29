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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCK_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCK_HPP_
#include <Experimental/Interpreter/Trace/Operation.hpp>
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
