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

#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
class RawTupleBuffer;

/// Type-erased wrapper around RawScan
class RawScanWrapper final
{
public:
    template <typename T>
    requires(not std::same_as<std::decay_t<T>, RawScanWrapper>)
    explicit RawScanWrapper(T&& RawScan)
        : RawScan(std::make_unique<RawScanModel<T>>(std::forward<T>(RawScan)))
    {
    }

    ~RawScanWrapper() = default;

    void scan(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child) const;

    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void stop(PipelineExecutionContext&) const;
    /// (concurrently) executes an RawScan.
    /// First, uses the concrete RawScanIndexer implementation to determine the indexes of all fields of all full tuples.
    /// Second, uses the SequenceShredder to find spanning tuples.
    /// Third, processes (leading) spanning tuple and if it contains at least two tuple delimiters and therefore one complete tuple,
    /// process all complete tuples and trailing spanning tuple.

    std::ostream& toString(std::ostream& os) const;

    /// Describes what a RawScan that is in the RawScanWrapper does (interface).
    struct RawScanConcept
    {
        virtual ~RawScanConcept() = default;
        virtual void stopTask() = 0;
        virtual void scanTask(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child) = 0;
        virtual std::ostream& toString(std::ostream& os) const = 0;
    };

    /// Defines the concrete behavior of the RawScanConcept, i.e., which specific functions from T to call.
    template <typename T>
    struct RawScanModel final : RawScanConcept
    {
        explicit RawScanModel(T&& RawScan) : RawScan(std::move(RawScan)) { }

        void stopTask() override { RawScan.stopTask(); }

        void scanTask(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child) override
        {
            RawScan.scanTask(executionCtx, recordBuffer, child);
        }

        std::ostream& toString(std::ostream& os) const override { return RawScan.taskToString(os); }

    private:
        T RawScan;
    };

    std::unique_ptr<RawScanConcept> RawScan;
};

}

FMT_OSTREAM(NES::RawScanWrapper);
