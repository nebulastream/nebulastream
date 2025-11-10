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
#include <utility>
#include <vector>

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
class RawTupleBuffer;

/// Type-erased wrapper around InputFormatter implementing the TupleBufferRef interface, enabling streamlined access to tuple buffers via
/// TupleBufferRefs/MemoryLayouts
class InputFormatterTupleBufferRef final : public NES::Nautilus::Interface::BufferRef::TupleBufferRef
{
public:
    template <typename T>
    requires(not std::same_as<std::decay_t<T>, InputFormatterTupleBufferRef>)
    explicit InputFormatterTupleBufferRef(T&& InputFormatter)
        /// As the InputFormatterTupleBufferRef are not using anything from the tuple buffer ref, we can simply add here dummy values
        : TupleBufferRef(0, 0, 0), InputFormatter(std::make_unique<InputFormatterModel<T>>(std::forward<T>(InputFormatter)))
    {
    }

    ~InputFormatterTupleBufferRef() override = default;

    std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const override
    {
        INVARIANT(false, "unsupported operation on InputFormatterBufferRef");
    }

    std::vector<DataType> getAllDataTypes() const override { INVARIANT(false, "unsupported operation on InputFormatterBufferRef"); }

    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    void
    writeRecord(nautilus::val<uint64_t>&, const RecordBuffer&, const Record&, const nautilus::val<AbstractBufferProvider*>&) const override
    {
        INVARIANT(false, "unsupported operation on InputFormatterBufferRef");
    }

    Interface::BufferRef::IndexBufferResult indexBuffer(RecordBuffer& recordBuffer, ArenaRef& arenaRef) override;

    friend std::ostream& operator<<(std::ostream& os, const InputFormatterTupleBufferRef& inputFormatterTupleBufferRef);

    /// Describes what a InputFormatter that is in the InputFormatterTupleBufferRef does (interface).
    struct InputFormatterConcept
    {
        virtual ~InputFormatterConcept() = default;
        virtual Record readRecord(
            const std::vector<Record::RecordFieldIdentifier>& projections,
            const RecordBuffer& recordBuffer,
            nautilus::val<uint64_t>& recordIndex) const
            = 0;
        virtual Interface::BufferRef::IndexBufferResult indexBuffer(RecordBuffer&, ArenaRef&) = 0;
        virtual std::ostream& toString(std::ostream& os) const = 0;
    };

    /// Defines the concrete behavior of the InputFormatterConcept, i.e., which specific functions from T to call.
    template <typename T>
    struct InputFormatterModel final : InputFormatterConcept
    {
        explicit InputFormatterModel(T&& InputFormatter) : InputFormatter(std::move(InputFormatter)) { }

        Interface::BufferRef::IndexBufferResult indexBuffer(RecordBuffer& recordBuffer, ArenaRef& arena) override
        {
            return InputFormatter.indexBuffer(recordBuffer, arena);
        }

        std::ostream& toString(std::ostream& os) const override { return InputFormatter.toString(os); }

        Record readRecord(
            const std::vector<Record::RecordFieldIdentifier>& projections,
            const RecordBuffer& recordBuffer,
            nautilus::val<uint64_t>& recordIndex) const override
        {
            return InputFormatter.readRecord(projections, recordBuffer, recordIndex);
        }

    private:
        T InputFormatter;
    };

    std::unique_ptr<InputFormatterConcept> InputFormatter;
};

}

FMT_OSTREAM(NES::InputFormatterTupleBufferRef);
