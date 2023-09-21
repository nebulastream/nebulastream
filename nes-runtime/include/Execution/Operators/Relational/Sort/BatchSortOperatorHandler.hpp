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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORT_BATCHSORTOPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORT_BATCHSORTOPERATORHANDLER_HPP_

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Sort operator handler that manages the state of the BatchSort and BatchSortScan operators.
 * It stores the tuples in a PagedVector that are sorted by the sort key when open() is called on BatchSortScan.
 */
class BatchSortOperatorHandler : public Runtime::Execution::OperatorHandler,
                                 public NES::detail::virtual_enable_shared_from_this<BatchSortOperatorHandler, false> {
  public:
    /**
     * @brief Creates the operator handler for the sort operator.
     * @param dataTypes data types of the records
     * @param fieldIdentifiers field identifiers of the records
     * @param sortFieldIdentifiers field identifiers of the records to sort
     */
    explicit BatchSortOperatorHandler(const std::vector<PhysicalTypePtr>& dataTypes,
                                      const std::vector<Record::RecordFieldIdentifier>& fieldIdentifiers,
                                      const std::vector<Record::RecordFieldIdentifier>& sortFieldIdentifiers) {
        NES_ASSERT(dataTypes.size() == fieldIdentifiers.size(), "Data types and field identifiers must have the same size");
        // Entry size is the sum of the sizes of the fields to sort, with will be stored encoded and the complete record
        entrySize = 0;
        // 1) calculate entry size of fields that we encode and store for sorting
        for (const auto& sortFieldIdentifier : sortFieldIdentifiers) {
            for (uint64_t j = 0; j < fieldIdentifiers.size(); ++j) {
                entrySize += (sortFieldIdentifier == fieldIdentifiers[j]) * dataTypes[j]->size();
            }
        }
        // 2) calculate entry size of complete record/payloads to store
        for (const auto& dataType : dataTypes) {
            entrySize += dataType->size();
        }
    };

    /**
     * @brief Sets up the state for the sort operator
     * @param ctx PipelineExecutionContext
     */
    void setup(Runtime::Execution::PipelineExecutionContext&) {
        auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
        stack = std::make_unique<Nautilus::Interface::PagedVector>(std::move(allocator), entrySize);
        auto tempAllocator = std::make_unique<NesDefaultMemoryAllocator>();
        tempStack = std::make_unique<Nautilus::Interface::PagedVector>(std::move(tempAllocator), entrySize);
    }

    /**
     * @brief Returns the state of the sort operator
     * @return Stack state
     */
    Nautilus::Interface::PagedVector* getState() const { return stack.get(); }

    /**
     * @brief Returns the temporary state of the sort operator
     * @return Stack state
     */
    Nautilus::Interface::PagedVector* getTempState() const { return tempStack.get(); }

    /**
     * @brief Returns the size of the entry
     * @return entry size
     */
    uint64_t getStateEntrySize() const { return entrySize; }

    void start(Runtime::Execution::PipelineExecutionContextPtr,  uint32_t) override{};

    void stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) override{};

  private:
    std::unique_ptr<Nautilus::Interface::PagedVector> stack;
    std::unique_ptr<Nautilus::Interface::PagedVector> tempStack;
    uint64_t entrySize;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORT_BATCHSORTOPERATORHANDLER_HPP_
