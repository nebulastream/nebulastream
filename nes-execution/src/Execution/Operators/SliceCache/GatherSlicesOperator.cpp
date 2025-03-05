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

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/SliceCache/GatherSlicesOperator.hpp>
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>

namespace NES::Runtime::Execution::Operators
{
void createAllSlices(
    const WindowBasedOperatorHandler* operatorHandler,
    const Memory::AbstractBufferProvider* bufferProvider,
    const Timestamp::Underlying* sliceStarts,
    const uint64_t numberOfSlices)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(bufferProvider != nullptr, "The buffer provider should not be null");
    for (uint64_t i = 0; i < numberOfSlices; i++)
    {
        const auto createFunction = operatorHandler->getCreateNewSlicesFunction(bufferProvider);
        operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(Timestamp(sliceStarts[i]), createFunction);
    }
}

class GatherSlicesOperatorLocalState final : public OperatorState
{
public:
    ~GatherSlicesOperatorLocalState() override = default;
    explicit GatherSlicesOperatorLocalState(nautilus::val<int8_t*> slice_ids_memory_area)
        : sliceStartsMemoryArea(std::move(slice_ids_memory_area)), numberOfSlicesInMemoryArea(0)
    {
    }

    nautilus::val<Timestamp::Underlying*> sliceStartsMemoryArea;
    nautilus::val<uint64_t> numberOfSlicesInMemoryArea;
};

GatherSlicesOperator::GatherSlicesOperator(
    const uint64_t operatorHandlerIndex, std::unique_ptr<TimeFunction> timeFunction, const uint64_t windowSize, const uint64_t windowSlide)
    : operatorHandlerIndex(operatorHandlerIndex), timeFunction(std::move(timeFunction)), windowSize(windowSize), windowSlide(windowSlide)
{
}

void GatherSlicesOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// call open on all child operators
    ExecutableOperator::open(executionCtx, recordBuffer);

    /// Initializing the time function
    timeFunction->open(executionCtx, recordBuffer);


    /// Allocating enough memory for the slice starts. We assume that each tuple has one slice start and that the slice start is a 64-bit integer.
    const auto numberOfRecords = recordBuffer.getNumRecords();
    const auto neededMemory = numberOfRecords * sizeof(uint64_t);
    const auto sliceStartsMemoryArea = executionCtx.allocateMemory(neededMemory);

    /// Adding the memory area to the operator state
    auto localState = std::make_unique<GatherSlicesOperatorLocalState>(sliceStartsMemoryArea);
    executionCtx.setLocalOperatorState(this, std::move(localState));
}

nautilus::val<Timestamp::Underlying> GatherSlicesOperator::getSliceStart(const nautilus::val<Timestamp>& ts) const
{
    const auto timestampRaw = ts.convertToValue();
    auto prevSlideStart = timestampRaw - ((timestampRaw) % windowSlide);
    auto prevWindowStart = timestampRaw < windowSize ? prevSlideStart : timestampRaw - ((timestampRaw - windowSize) % windowSlide);
    if (prevSlideStart > prevWindowStart)
    {
        return prevSlideStart;
    }
    return prevWindowStart;
}

void GatherSlicesOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Getting the local state
    const auto localState = dynamic_cast<GatherSlicesOperatorLocalState*>(executionCtx.getLocalState(this));

    /// Getting the timestamp of the record and then calculating the slice start from it
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto sliceStart = getSliceStart(timestamp);

    /// Adding the timestamp to the slice ids memory area, only if the timestamp is not already in the memory area
    nautilus::val<bool> found = false;
    for (nautilus::val<uint64_t> i = 0; i < localState->numberOfSlicesInMemoryArea; i = i + 1)
    {
        const nautilus::val<Timestamp::Underlying> curSliceStart = *(localState->sliceStartsMemoryArea + i);
        if (curSliceStart == timestamp.convertToValue())
        {
            found = true;
            break;
        }
    }
    if (not found)
    {
        *(localState->sliceStartsMemoryArea + localState->numberOfSlicesInMemoryArea) = timestamp.convertToValue();
        localState->numberOfSlicesInMemoryArea = localState->numberOfSlicesInMemoryArea + 1;
    }
}

void GatherSlicesOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Getting the local state and creating all slices from the slice starts
    const auto localState = dynamic_cast<GatherSlicesOperatorLocalState*>(executionCtx.getLocalState(this));
    const auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    nautilus::invoke(
        createAllSlices,
        operatorHandler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        localState->sliceStartsMemoryArea,
        localState->numberOfSlicesInMemoryArea);

    /// Giving the chance for any child operator to close its state
    ExecutableOperator::close(executionCtx, recordBuffer);
}
}