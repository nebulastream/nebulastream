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

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <CompilationContext.hpp>
#include <EmitOperatorHandler.hpp>
#include <PipelineExecutionContext.hpp>
#include <RangeCoalescer.hpp>
#include <val_details.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Appends the records of the source to the target buffer through `bufferRef`, whose layout both buffers share. The target must have room
/// for them and keeps its tuple count if writing a record throws.
std::function<void(nautilus::val<TupleBuffer*>, nautilus::val<const TupleBuffer*>, nautilus::val<AbstractBufferProvider*>)>
mergeRecordsFunction(const std::shared_ptr<TupleBufferRef>& bufferRef);

/// Hands the complete output of each unchunked input to a `RangeCoalescer`, which merges the outputs of adjacent inputs. Chunked and spilled
/// outputs leave as from `EmitOperatorHandler`. Its stop emits all outputs the coalescer still holds.
class CoalescingEmitOperatorHandler final : public EmitOperatorHandler
{
public:
    /// `bufferRef` is the emit's, and `canReadBack` holds for it.
    CoalescingEmitOperatorHandler(std::shared_ptr<TupleBufferRef> bufferRef, std::chrono::microseconds maxDelay, size_t maxHeldRuns);

    /// Whether `bufferRef` can read back the records it wrote, as the merge does. Only the row and columnar layouts can.
    [[nodiscard]] static bool canReadBack(const TupleBufferRef& bufferRef);

    /// Registers the merge of `bufferRef` with the pipeline and creates the coalescer.
    void setup(CompilationContext& compilationCtx, PipelineExecutionContext& pec) override;
    void stop(QueryTerminationType, PipelineExecutionContext& pec) override;

protected:
    void emitCompleteOutput(TupleBuffer& output, PipelineExecutionContext& pec) override;

private:
    std::shared_ptr<TupleBufferRef> bufferRef;
    std::chrono::microseconds maxDelay;
    size_t maxHeldRuns;
    /// Created by `setup`, as its merge is compiled with the pipeline.
    std::optional<RangeCoalescer> coalescer;
};

}
