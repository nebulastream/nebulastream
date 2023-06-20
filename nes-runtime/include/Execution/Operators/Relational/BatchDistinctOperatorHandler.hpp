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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHDISTINCTOPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHDISTINCTOPERATORHANDLER_HPP_

#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Sort operator handler that manages the state of the BatchDistinct and BatchDistinctScan operators.
 */
class BatchDistinctOperatorHandler : public Runtime::Execution::OperatorHandler,
                                 public NES::detail::virtual_enable_shared_from_this<BatchDistinctOperatorHandler, false> {
  public:
    /**
     * @brief Creates the operator handler for the DISTINCT operator.
     * @param entrySize size of the entry
     */
    explicit BatchDistinctOperatorHandler(uint64_t entrySize) : entrySize(entrySize){};

    /**
     * @brief Sets up the state for the sort operator
     * @param ctx PipelineExecutionContext
     */
    void setup(Runtime::Execution::PipelineExecutionContext&) {
        //Todo: Can this stay the same?
        auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
        stack = std::make_unique<Nautilus::Interface::PagedVector>(std::move(allocator), entrySize);
        auto tempAllocator = std::make_unique<NesDefaultMemoryAllocator>();
        tempStack = std::make_unique<Nautilus::Interface::PagedVector>(std::move(tempAllocator), entrySize);
    }

    /**
     * @brief Returns the state of the DISTINCT operator (Todo: needed?)
     * @return Stack state
     */
    Nautilus::Interface::PagedVector* getState() const { return stack.get(); }

    /**
     * @brief Returns the temporary state of the DISTINCT operator (Todo: needed?)
     * @return Stack state
     */
    Nautilus::Interface::PagedVector* getTempState() const { return tempStack.get(); }

    /**
     * @brief Returns the size of the entry (Todo: needed?)
     * @return entry size
     */
    uint64_t getStateEntrySize() const { return entrySize; }

    void start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) override{};

    void stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) override{};

  private:
    std::unique_ptr<Nautilus::Interface::PagedVector> stack;
    std::unique_ptr<Nautilus::Interface::PagedVector> tempStack;
    uint64_t entrySize;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHDISTINCTOPERATORHANDLER_HPP_
