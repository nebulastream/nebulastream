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

#include <concepts>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
class RawTupleBuffer;

/// Type-erased wrapper around InputFormatter implementing the TupleBufferRef interface, enabling streamlined access to tuple buffers via
/// TupleBufferRefs/MemoryLayouts
class InputFormatterTupleBufferRef final : public TupleBufferRef
{
    using ExecuteChildFn = std::function<void(ExecutionContext& executionCtx, Record& record)>;

public:
    template <typename T>
    requires(not std::same_as<std::decay_t<T>, InputFormatterTupleBufferRef>)
    explicit InputFormatterTupleBufferRef(T&& inputFormatter)
        : TupleBufferRef(0, 0, 0), inputFormatter(std::make_unique<InputFormatterModel<T>>(std::forward<T>(inputFormatter)))
    {
    }

    ~InputFormatterTupleBufferRef() override = default;

    [[nodiscard]] std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const override
    {
        INVARIANT(false, "unsupported operation on InputFormatterBufferRef");
        std::unreachable();
    }

    [[nodiscard]] std::vector<DataType> getAllDataTypes() const override
    {
        INVARIANT(false, "unsupported operation on InputFormatterBufferRef");
        std::unreachable();
    }

    Record readRecord(const std::vector<Record::RecordFieldIdentifier>&, const RecordBuffer&, nautilus::val<uint64_t>&) const override
    {
        INVARIANT(false, "Does not implement 'readRecord()'");
        std::unreachable();
    }

    void readBuffer(ExecutionContext& executionCtx, const RecordBuffer& recordBuffer, const ExecuteChildFn& executeChild) const;

    nautilus::val<size_t>
    writeRecord(nautilus::val<uint64_t>&, const RecordBuffer&, const Record&, const nautilus::val<AbstractBufferProvider*>&, nautilus::val<bool>) const override
    {
        INVARIANT(false, "unsupported operation on InputFormatterBufferRef");
        std::unreachable();
    }

    nautilus::val<bool> indexBuffer(RecordBuffer& recordBuffer, ArenaRef& arenaRef) const;

    friend std::ostream& operator<<(std::ostream& os, const InputFormatterTupleBufferRef& inputFormatterTupleBufferRef);

    /// Describes what a InputFormatter that is in the InputFormatterTupleBufferRef does (interface).
    struct InputFormatterConcept
    {
        virtual ~InputFormatterConcept() = default;
        virtual void readBuffer(ExecutionContext& executionCtx, const RecordBuffer& recordBuffer, const ExecuteChildFn& executeChild) const
            = 0;
        virtual nautilus::val<bool> indexBuffer(RecordBuffer&, ArenaRef&) const = 0;
        virtual std::ostream& toString(std::ostream& os) const = 0;
    };

    /// Defines the concrete behavior of the InputFormatterConcept, i.e., which specific functions from T to call.
    template <typename T>
    struct InputFormatterModel final : InputFormatterConcept
    {
        explicit InputFormatterModel(T&& inputFormatter) : InputFormatter(std::move(inputFormatter)) { }

        nautilus::val<bool> indexBuffer(RecordBuffer& recordBuffer, ArenaRef& arena) const override
        {
            return InputFormatter.indexBuffer(recordBuffer, arena);
        }

        std::ostream& toString(std::ostream& os) const override { return InputFormatter.toString(os); }

        void readBuffer(ExecutionContext& executionCtx, const RecordBuffer& recordBuffer, const ExecuteChildFn& executeChild) const override
        {
            return InputFormatter.readBuffer(executionCtx, recordBuffer, executeChild);
        }

    private:
        T InputFormatter;
    };

    std::unique_ptr<InputFormatterConcept> inputFormatter;
};

}

FMT_OSTREAM(NES::InputFormatterTupleBufferRef);
