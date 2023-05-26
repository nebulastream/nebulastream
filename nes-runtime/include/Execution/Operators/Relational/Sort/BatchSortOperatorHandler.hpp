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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTOPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTOPERATORHANDLER_HPP_

#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Nautilus/Interface/Stack/StackRef.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Sort operator handler that manages the state of the BatchSort and BatchSortScan operators.
 */
class BatchSortOperatorHandler : public Runtime::Execution::OperatorHandler,
                         public NES::detail::virtual_enable_shared_from_this<BatchSortOperatorHandler, false> {
  public:
    /**
     * @brief Creates the operator handler for the sort operator.
     * @param entrySize size of the entry
     */
    explicit BatchSortOperatorHandler(uint64_t entrySize) : entrySize(entrySize) {};

    /**
     * @brief Sets up the state for the sort operator
     * @param ctx PipelineExecutionContext
     */
    void setup(Runtime::Execution::PipelineExecutionContext&) {
        auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
        stack = std::make_unique<Nautilus::Interface::Stack>(std::move(allocator), entrySize);
        auto tempAllocator = std::make_unique<NesDefaultMemoryAllocator>();
        tempStack = std::make_unique<Nautilus::Interface::Stack>(std::move(tempAllocator), entrySize);
    }

    /**
     * @brief Returns the state of the sort operator
     * @return Stack state
     */
    Nautilus::Interface::Stack *getState() { return stack.get(); }

    /**
     * @brief Returns the temporary state of the sort operator
     * @return Stack state
     */
    Nautilus::Interface::Stack *getTempState() { return tempStack.get(); }

    /**
     * @brief Returns the size of the entry
     * @return entry size
     */
    uint64_t getStateEntrySize() const { return entrySize; }


    void start(Runtime::Execution::PipelineExecutionContextPtr,
               Runtime::StateManagerPtr,
               uint32_t) override {};

    void stop(Runtime::QueryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr) override {};
  private:
    std::unique_ptr<Nautilus::Interface::Stack> stack;
    std::unique_ptr<Nautilus::Interface::Stack> tempStack;
    uint64_t entrySize;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTOPERATORHANDLER_HPP_
