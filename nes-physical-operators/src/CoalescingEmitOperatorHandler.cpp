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

#include <CoalescingEmitOperatorHandler.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <Interface/BufferRef/ColumnTupleBufferRef.hpp>
#include <Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <RangeCoalescer.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_details.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
void mergeDoesNotFitProxy(const uint64_t IF_PRECONDITION(numberOfRecords), const uint64_t IF_PRECONDITION(capacity))
{
    PRECONDITION(false, "A merge needs room for {} records, but the target holds {}", numberOfRecords, capacity);
}
}

std::function<void(nautilus::val<TupleBuffer*>, nautilus::val<const TupleBuffer*>, nautilus::val<AbstractBufferProvider*>)>
mergeRecordsFunction(const std::shared_ptr<TupleBufferRef>& bufferRef)
{
    return [bufferRef, fields = bufferRef->getAllFieldNames()](
               const nautilus::val<TupleBuffer*>& target,
               const nautilus::val<const TupleBuffer*>& source,
               const nautilus::val<AbstractBufferProvider*>& provider)
    {
        const RecordBuffer from{BorrowedNautilusBuffer::from(source)};
        RecordBuffer into{BorrowedNautilusBuffer::from(target)};
        const auto numberOfRecords = from.getNumRecords();
        auto index = into.getNumRecords();
        const auto required = index + numberOfRecords;
        const nautilus::val<uint64_t> capacity{bufferRef->getCapacity()};
        if (required > capacity)
        {
            nautilus::invoke(mergeDoesNotFitProxy, required, capacity);
        }
        for (nautilus::val<uint64_t> i = 0; i < numberOfRecords; i = i + 1)
        {
            bufferRef->writeRecord(index, into, bufferRef->readRecord(fields, from, i), provider);
            index = index + 1;
        }
        into.setNumRecords(index);
    };
}

CoalescingEmitOperatorHandler::CoalescingEmitOperatorHandler(
    std::shared_ptr<TupleBufferRef> bufferRef, const std::chrono::microseconds maxDelay, const size_t maxHeldRuns)
    : bufferRef(std::move(bufferRef)), maxDelay(maxDelay), maxHeldRuns(maxHeldRuns)
{
    PRECONDITION(canReadBack(*this->bufferRef), "A coalescing emit needs a buffer ref that can read back its records");
}

bool CoalescingEmitOperatorHandler::canReadBack(const TupleBufferRef& bufferRef)
{
    return dynamic_cast<const RowTupleBufferRef*>(&bufferRef) != nullptr
        || dynamic_cast<const ColumnTupleBufferRef*>(&bufferRef) != nullptr;
}

void CoalescingEmitOperatorHandler::setup(CompilationContext& compilationCtx, PipelineExecutionContext& pec)
{
    PRECONDITION(!coalescer.has_value(), "The coalescer is set up once");
    const auto merge = compilationCtx.registerFunction(mergeRecordsFunction(bufferRef), "coalesceMerge");
    coalescer.emplace(
        bufferRef->getCapacity(),
        maxDelay,
        maxHeldRuns,
        [merge, provider = pec.getBufferManager()](TupleBuffer& target, const TupleBuffer& source)
        { merge(&target, &source, provider.get()); });
}

void CoalescingEmitOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext& pec)
{
    INVARIANT(coalescer.has_value(), "The pipeline is only stopped after its setup created the coalescer");
    coalescer->flushAll(pec);
}

void CoalescingEmitOperatorHandler::emitCompleteOutput(TupleBuffer& output, PipelineExecutionContext& pec)
{
    INVARIANT(coalescer.has_value(), "The pipeline's setup creates the coalescer before the first output");
    coalescer->offer(output, pec, RangeCoalescer::Clock::now());
}

}
