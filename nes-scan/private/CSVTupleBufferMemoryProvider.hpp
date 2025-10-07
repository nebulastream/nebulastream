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
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Util/Pointers.hpp>
#include <CSVInputFormatIndexer.hpp>
#include <ExecutionContext.hpp>

namespace NES::Nautilus::Interface::MemoryProvider
{

/// Implements MemoryProvider. Provides columnar memory access.
class CSVTupleBufferMemoryProvider final : public TupleBufferMemoryProvider
{
    std::unique_ptr<SequenceShredder> shredder;
    /// metadata is a pointer because I am afraid that moving the MemoryProvider would break JIT compiled code because the address if
    /// metadata is no longer valid
    std::unique_ptr<CSVMetaData> metadata;
    /// I don't know for some reason we need a memory layout.
    std::shared_ptr<MemoryLayout> layout;
public:
    /// Creates a column memory provider based on a valid column memory layout pointer.
    CSVTupleBufferMemoryProvider(ParserConfig config, std::shared_ptr<MemoryLayout> layout);
    ~CSVTupleBufferMemoryProvider() override = default;

    [[nodiscard]] std::shared_ptr<MemoryLayout> getMemoryLayout() const override;

    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    void writeRecord(
        nautilus::val<uint64_t>& recordIndex,
        const RecordBuffer& recordBuffer,
        const Record& rec,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;

    void open(const RecordBuffer& recordBuffer, ArenaRef& arenaRef) override;

    nautilus::val<uint64_t> getNumberOfRecords(const RecordBuffer& recordBuffer) const override;
};

}
