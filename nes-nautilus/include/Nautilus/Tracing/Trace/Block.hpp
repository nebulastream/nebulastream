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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_BLOCK_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_BLOCK_HPP_
#include <Nautilus/Tracing/Trace/TraceOperation.hpp>
#include <cinttypes>
#include <ostream>
#include <vector>
namespace NES::Nautilus::Tracing {

/**
 * @brief Represents a basic block in a trace
 */
class Block {
  public:
    /**
     * @brief Indicates if this is a control flow merge block -> such blocks could be loop headers.
     */
    enum class Type : uint8_t { Default, ControlFlowMerge };

    /**
     * @brief creates a new block with a specific block id.
     * @param blockId
     */
    Block(uint32_t blockId);

    /**
     * @brief Check if this block defines a specific ValueRef as a parameter or result of an operation
     * @param ref
     * @return
     */
    bool isLocalValueRef(ValueRef& ref);
    /**
     * @brief Adds a argument to the block
     * @param value reference
     */
    void addArgument(ValueRef ref);
    friend std::ostream& operator<<(std::ostream& os, const Block& block);

    /**
     * @brief defines the id of this block.
     */
    uint32_t blockId;

    /**
     * @brief defines the type of this block.
     */
    Type type;

    /**
     * @brief indicates a list of arguments that this block receives.
     */
    std::vector<ValueRef> arguments;

    /**
     * @brief Defines a list of operations this block contains.
     */
    std::vector<TraceOperation> operations;

    /**
     * @brief Indicates successors of this block.
     */
    std::vector<uint32_t> predecessors;
};

}// namespace NES::Nautilus::Tracing

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_BLOCK_HPP_
