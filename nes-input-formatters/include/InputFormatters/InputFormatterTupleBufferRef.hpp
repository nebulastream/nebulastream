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
#include <ostream>
#include <vector>
#include <utility>

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
class RawTupleBuffer;

/// Type-erased wrapper around InputFormatter
class InputFormatterTupleBufferRef final : public NES::Nautilus::Interface::BufferRef::TupleBufferRef
{
public:
    template <typename T>
    requires(not std::same_as<std::decay_t<T>, InputFormatterTupleBufferRef>)
    explicit InputFormatterTupleBufferRef(T&& InputFormatter)
        : InputFormatter(std::make_unique<InputFormatterModel<T>>(std::forward<T>(InputFormatter)))
    {
    }

    ~InputFormatterTupleBufferRef() override = default;

    [[nodiscard]] std::shared_ptr<MemoryLayout> getMemoryLayout() const override;

    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    void writeRecord(
        nautilus::val<uint64_t>&,
        const RecordBuffer&,
        const Record&,
        const nautilus::val<AbstractBufferProvider*>&) const override
    {
        INVARIANT(false, "unsupported operation on InputFormatterBufferRef");
    }

    void open(RecordBuffer& recordBuffer, ArenaRef& arenaRef) override;

    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void close(PipelineExecutionContext&) const;

    std::ostream& toString(std::ostream& os) const;

    /// Describes what a InputFormatter that is in the InputFormatterTupleBufferRef does (interface).
    struct InputFormatterConcept
    {
        virtual ~InputFormatterConcept() = default;
        virtual void close() = 0;
        virtual Record readRecord(
            const std::vector<Record::RecordFieldIdentifier>& projections,
            const RecordBuffer& recordBuffer,
            nautilus::val<uint64_t>& recordIndex) const
            = 0;
        virtual void open(RecordBuffer&, ArenaRef&) = 0;
        virtual std::shared_ptr<MemoryLayout> getMemoryLayout() const = 0;
        // virtual nautilus::val<uint64_t> getNumberOfRecords(const RecordBuffer& recordBuffer) const = 0;
        virtual std::ostream& toString(std::ostream& os) const = 0;
    };

    /// Defines the concrete behavior of the InputFormatterConcept, i.e., which specific functions from T to call.
    template <typename T>
    struct InputFormatterModel final : InputFormatterConcept
    {
        explicit InputFormatterModel(T&& InputFormatter) : InputFormatter(std::move(InputFormatter)) { }

        // Todo: what to do with 'stopTask'? <-- close?
        void close() override { InputFormatter.close(); }

        void open(RecordBuffer& recordBuffer, ArenaRef& arena) override { InputFormatter.open(recordBuffer, arena); }
        // nautilus::val<uint64_t> getNumberOfRecords(const RecordBuffer& recordBuffer) const override{ return InputFormatter.getNumberOfRecords(recordBuffer); };

        std::ostream& toString(std::ostream& os) const override { return InputFormatter.taskToString(os); }

        Record readRecord(
            const std::vector<Record::RecordFieldIdentifier>& projections,
            const RecordBuffer& recordBuffer,
            nautilus::val<uint64_t>& recordIndex) const override
        {
            return InputFormatter.readRecord(projections,recordBuffer, recordIndex);
        }

        [[nodiscard]] std::shared_ptr<MemoryLayout> getMemoryLayout() const override { return InputFormatter.getMemoryLayout(); }

    private:
        T InputFormatter;
    };

    std::unique_ptr<InputFormatterConcept> InputFormatter;
};

}

FMT_OSTREAM(NES::InputFormatterTupleBufferRef);
