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

#include <Execution/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>

namespace NES::Runtime::Execution::MemoryProvider
{

/// Implements MemoryProvider. Provides row-wise memory access.
class RowTupleBufferMemoryProvider final : public TupleBufferMemoryProvider
{
public:
    /// Creates a row memory provider based on a valid row memory layout pointer.
    RowTupleBufferMemoryProvider(std::shared_ptr<Memory::MemoryLayouts::RowLayout> rowMemoryLayoutPtr);
    ~RowTupleBufferMemoryProvider() override = default;

    Memory::MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() override;

    Nautilus::Record readRecord(
        const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
        nautilus::val<int8_t*>& bufferAddress,
        nautilus::val<uint64_t>& recordIndex) const override;

    void
    writeRecord(nautilus::val<uint64_t>& recordIndex, nautilus::val<int8_t*>& bufferAddress, NES::Nautilus::Record& rec) const override;

private:
    [[nodiscard]] nautilus::val<int8_t*> calculateFieldAddress(const nautilus::val<int8_t*>& recordOffset, const uint64_t fieldIndex) const;

    std::shared_ptr<Memory::MemoryLayouts::RowLayout> rowMemoryLayoutPtr;
};

}
