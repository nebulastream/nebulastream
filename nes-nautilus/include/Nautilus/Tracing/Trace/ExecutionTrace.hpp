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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_EXECUTIONTRACE_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_EXECUTIONTRACE_HPP_

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
    /**
     * @brief Adds an operation to the current block
     * @param operation TraceOperation
     */
    void addOperation(TraceOperation& operation);

    /**
     * @brief Adds arguments that are passed to the traced function
     * @param argument
     */
    void addArgument(const ValueRef& argument);

    /**
     * @brief Returns all arguments of this trace.
     * @return std::vector<ValueRef>
     */
    const std::vector<ValueRef>& getArguments();

    /**
     * @brief Creates a new block in the trace.
     * @return
     */
    uint32_t createBlock();

    /**
     * @brief Returns the reference to a specific block
     * @param blockIndex
     * @return Block&
     */
    Block& getBlock(uint32_t blockIndex) { return blocks[blockIndex]; }

    /**
     * @brief Returns a reference to all blocks
     * @return std::vector<Block>&
     */
    std::vector<Block>& getBlocks() { return blocks; }

    /**
     * @brief Returns the index to the current block.
     * @return uint32_t
     */
    uint32_t getCurrentBlockIndex() const { return currentBlock; }

    /**
     * @brief Returns the current block
     * @return Block&
     */
    Block& getCurrentBlock() { return blocks[currentBlock]; }

    /**
     * @brief Sets the current block
     * @param index
     */
    void setCurrentBlock(uint32_t index) { currentBlock = index; }

    /**
     * @brief Processes a control flow merge
     * @param blockIndex
     * @param operationIndex
     * @return Block&
     */
    Block& processControlFlowMerge(uint32_t blockIndex, uint32_t operationIndex);

    /**
     * @brief Returns the return reference
     * @return  std::shared_ptr<OperationRef>
     */
    std::shared_ptr<OperationRef> getReturn();

    std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const ExecutionTrace& tag);

  private:
    uint32_t currentBlock;
    std::vector<Block> blocks;
    std::vector<ValueRef> arguments;
    std::shared_ptr<OperationRef> returnRef;
};

}// namespace NES::Nautilus::Tracing

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_EXECUTIONTRACE_HPP_
