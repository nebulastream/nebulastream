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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <span>
#include <utility>
#include <vector>

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Arena.hpp>
#include <ExecutionContext.hpp>
#include <SequenceShredder.hpp>
#include <val.hpp>

#include <DataTypes/DataType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <InputFormatIndexer.hpp>
#include <RawTupleBuffer.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// The type that all formatters use to represent indexes to fields.
using FieldIndex = uint32_t;

/// Defined at NES namespace scope in InputFormatter.cpp. Intentionally opaque to external TUs: only InputFormatter's own member functions
/// (whose bodies live in that .cpp) need the full definition.
struct IndexPhaseResult;

/// InputFormatters concurrently take (potentially) raw input buffers and format all full tuples in these raw input buffers that the
/// individual InputFormatters see during execution.
/// The only point of synchronization is a call to the SequenceShredder data structure, which determines which buffers the InputFormatter
/// needs to process (resolving tuples that span multiple raw buffers).
/// An InputFormatter belongs to exactly one source. The source reads raw data into buffers, constructs a Task from the
/// raw buffer and its successor (the InputFormatter) and writes it to the task queue of the QueryEngine.
/// The QueryEngine concurrently executes InputFormatters. Thus, even if the source writes the InputFormatters to the task queue sequentially,
/// the QueryEngine may still execute them in any order.
class InputFormatter : public TupleBufferRef
{
public:
    explicit InputFormatter(
        std::unique_ptr<InputFormatIndexer> inputFormatIndexer,
        std::shared_ptr<TupleBufferRef> memoryProvider,
        const ParserConfig& parserConfig)
        : TupleBufferRef(*memoryProvider)
        , inputFormatIndexer(std::move(inputFormatIndexer))
        , projections(memoryProvider->getAllFieldNames())
        , memoryProvider(std::move(memoryProvider))
        , sequenceShredder(std::make_unique<SequenceShredder>(parserConfig.tupleDelimiter.size()))
    {
    }

    ~InputFormatter() override = default;
    [[nodiscard]] std::vector<DataType> getAllDataTypes() const override;
    InputFormatter(const InputFormatter&) = delete;
    InputFormatter& operator=(const InputFormatter&) = delete;
    InputFormatter(InputFormatter&&) = default;
    InputFormatter& operator=(InputFormatter&&) = delete;

    Record readRecord(const std::vector<Record::RecordFieldIdentifier>&, const RecordBuffer&, nautilus::val<uint64_t>&) const override;

    WriteRecordResult
    writeRecord(nautilus::val<uint64_t>&, const RecordBuffer&, const Record&, const nautilus::val<AbstractBufferProvider*>&) const override;

    [[nodiscard]] std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const override;

    /// Executes the first phase, which indexes a (raw) buffer enabling the second phase, which calls 'readBuffer()' to index specific
    /// records/fields within the (raw) buffer. Relies on static thread_local member variables to 'bridge' the result of the indexing phase
    /// to the second phase, which uses the index to access specific records/fields
    [[nodiscard]] nautilus::val<bool> indexBuffer(const RecordBuffer& recordBuffer, const ArenaRef& arenaRef) const;

    /// Executes the second phase, which iterates over a (raw) buffer, reading specific records and fields from a (raw) buffer
    /// Relies on the index created in the first phase (indexBuffer), which it accesses through the static_thread local member
    void readBuffer(
        ExecutionContext& executionCtx,
        const RecordBuffer& recordBuffer,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild);


    std::ostream& toString(std::ostream& os) const;

private:
    std::unique_ptr<InputFormatIndexer> inputFormatIndexer;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::shared_ptr<TupleBufferRef> memoryProvider;
    std::unique_ptr<SequenceShredder> sequenceShredder;
};

}
