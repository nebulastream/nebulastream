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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_EXECUTIONTRACE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_EXECUTIONTRACE_HPP_
#include <Experimental/Interpreter/Trace/Block.hpp>
#include <Experimental/Interpreter/Trace/Tag.hpp>
#include <memory>
namespace NES::Interpreter {

class ExecutionTrace {
  public:
    ExecutionTrace();
    ~ExecutionTrace() = default;
    void addOperation(Operation& operation);

    uint32_t createBlock();

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

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_EXECUTIONTRACE_HPP_
