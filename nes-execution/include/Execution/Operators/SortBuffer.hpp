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

#pragma once

#include <Execution/Operators/Operator.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>

namespace NES::Runtime::Execution::Operators
{

enum class SortOrder : uint8_t
{
    Ascending,
    Descending
};

/**
 * @brief Sort operator for tuple buffers.
 * This operator takes a tuple buffer, sorts the tuples by a given field and emits them.
 */
class SortBuffer : public Operator
{
public:
    /**
     * @brief Construct a new SortBuffer operator
     *
     * @param dataTypes data types of the input tuples
     * @param fieldIdentifiers
     * @param sortFieldIdentifier
     */
    SortBuffer(
        std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider,
        const Record::RecordFieldIdentifier& sortFieldIdentifier,
        const SortOrder sortOrder = SortOrder::Ascending);

    void setup(ExecutionContext& executionCtx) const override;

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    void emitRecordBuffer(ExecutionContext& ctx, RecordBuffer& inputBuffer, RecordBuffer& ouputBuffer) const;

    std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
    const Record::RecordFieldIdentifier sortFieldIdentifier;
    const SortOrder sortOrder;
    std::vector<Record::RecordFieldIdentifier> projections;
};

} // namespace NES::Runtime::Execution::Operators