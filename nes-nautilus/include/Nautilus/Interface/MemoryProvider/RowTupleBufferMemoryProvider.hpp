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

#include <memory>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>

namespace NES::Nautilus::Interface::MemoryProvider
{

/// Implements MemoryProvider. Provides row-wise memory access.
class RowTupleBufferMemoryProvider final : public TupleBufferMemoryProvider
{
public:
    /// Creates a row memory provider based on a valid row memory layout pointer.
    RowTupleBufferMemoryProvider(std::shared_ptr<Memory::MemoryLayouts::RowLayout> rowMemoryLayoutPtr);
    ~RowTupleBufferMemoryProvider() override = default;

    [[nodiscard]] std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> getMemoryLayout() const override;

    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    void writeRecord(
        nautilus::val<uint64_t>& recordIndex,
        const RecordBuffer& recordBuffer,
        const Record& rec,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) const override;

private:
    [[nodiscard]] nautilus::val<int8_t*> calculateFieldAddress(const nautilus::val<int8_t*>& recordOffset, const uint64_t fieldIndex) const;

    /// It is fine that we are storing here a non nautilus value, as we are only calling methods, which return values stay
    /// the same, during tracing and during the execution of the generated code.
    std::shared_ptr<Memory::MemoryLayouts::RowLayout> rowMemoryLayout;
};

}
