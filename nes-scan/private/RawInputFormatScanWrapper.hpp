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
#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
class RawTupleBuffer;

/// Type-erased wrapper around RawInputFormatScan
class RawInputFormatScanWrapper final
{
public:
    template <typename T>
    requires(not std::same_as<std::decay_t<T>, RawInputFormatScanWrapper>)
    explicit RawInputFormatScanWrapper(T&& rawInputFormatScan)
        : rawInputFormatScan(std::make_unique<RawInputFormatScanModel<T>>(std::forward<T>(rawInputFormatScan)))
    {
    }

    ~RawInputFormatScanWrapper() = default;

    OpenReturnState scan(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child) const;

    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void stop(PipelineExecutionContext&) const;
    /// (concurrently) executes an RawInputFormatScan.
    /// First, uses the concrete InputFormatIndexer implementation to determine the indexes of all fields of all full tuples.
    /// Second, uses the SequenceShredder to find spanning tuples.
    /// Third, processes (leading) spanning tuple and if it contains at least two tuple delimiters and therefore one complete tuple,
    /// process all complete tuples and trailing spanning tuple.

    std::ostream& toString(std::ostream& os) const;

    /// Describes what a RawInputFormatScan that is in the RawInputFormatScanWrapper does (interface).
    struct RawInputFormatScanConcept
    {
        virtual ~RawInputFormatScanConcept() = default;
        virtual void stopTask() = 0;
        virtual OpenReturnState
        scanTask(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child)
            = 0;
        virtual std::ostream& toString(std::ostream& os) const = 0;
    };

    /// Defines the concrete behavior of the RawInputFormatScanConcept, i.e., which specific functions from T to call.
    template <typename T>
    struct RawInputFormatScanModel final : RawInputFormatScanConcept
    {
        explicit RawInputFormatScanModel(T&& rawInputFormatScan) : rawInputFormatScan(std::move(rawInputFormatScan)) { }

        void stopTask() override { rawInputFormatScan.stopTask(); }

        OpenReturnState
        scanTask(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child) override
        {
            return rawInputFormatScan.scanTask(executionCtx, recordBuffer, child);
        }

        std::ostream& toString(std::ostream& os) const override { return rawInputFormatScan.taskToString(os); }

    private:
        T rawInputFormatScan;
    };

    std::unique_ptr<RawInputFormatScanConcept> rawInputFormatScan;
};

}

FMT_OSTREAM(NES::RawInputFormatScanWrapper);
