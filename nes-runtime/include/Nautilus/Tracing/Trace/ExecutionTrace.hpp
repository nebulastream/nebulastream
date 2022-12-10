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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACE_EXECUTIONTRACE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACE_EXECUTIONTRACE_HPP_

#include <Nautilus/Tracing/Tag/TagRecorder.hpp>
#include <Nautilus/Tracing/Trace/Block.hpp>
#include <memory>
#include <unordered_map>
namespace NES::Nautilus::Tracing {

/**
 * @brief The execution trace captures the trace of a program
 */
class ExecutionTrace {
  public:
    ExecutionTrace();
    ~ExecutionTrace() = default;
    void addOperation(TraceOperation& operation);
    void addArgument(const ValueRef& argument);

    uint32_t createBlock();

    Block& getBlock(uint32_t blockIndex) { return blocks[blockIndex]; }
    std::vector<Block>& getBlocks() { return blocks; }
    Block& getCurrentBlock() { return blocks[currentBlock]; }
    uint32_t getCurrentBlockIndex() { return currentBlock; }

    bool hasNextOperation() { return currentBlock; }

    void setCurrentBlock(uint32_t index) { currentBlock = index; }
    Block& processControlFlowMerge(uint32_t blockIndex, uint32_t operationIndex);

    std::shared_ptr<OperationRef> findKnownOperation(const Tag* tag);

    friend std::ostream& operator<<(std::ostream& os, const ExecutionTrace& tag);

    std::unordered_map<const Tag*, std::shared_ptr<OperationRef>> tagMap;
    std::unordered_map<const Tag*, std::shared_ptr<OperationRef>> localTagMap;

    std::shared_ptr<OperationRef> returnRef;
    std::vector<ValueRef> getArguments();

  private:
    uint32_t currentBlock;
    std::vector<Block> blocks;
    std::vector<ValueRef> arguments;
};

}// namespace NES::Nautilus::Tracing

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACE_EXECUTIONTRACE_HPP_
