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
#include <Aggregation/Function/ArrayAggPageSortedAggregationPhysicalFunction.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include "Identifiers/Identifiers.hpp"
#include "DataTypes/DataType.hpp"
#include "Functions/PhysicalFunction.hpp"
#include "Interface/Record.hpp"
#include "Aggregation/Function/AggregationPhysicalFunction.hpp"
#include "Interface/TimestampRef.hpp"
#include "Time/Timestamp.hpp"

namespace NES
{
namespace
{
struct PageHeader
{
    uint64_t firstTimestamp;
    uint64_t numberOfValues;
};

struct PageSortedArrayAggState
{
    ChildBufferIndex::Underlying pagesIndex = 0;
    int8_t* currentPageData = nullptr;
    PageHeader* currentPageHeader = nullptr;
    uint64_t writeOffset = 0;
    uint64_t pageSize = 0;
    uint64_t currentOriginId = 0;
    uint64_t currentSequenceNumber = 0;
    uint64_t currentChunkNumber = 0;
    bool hasCurrentInputBuffer = false;
};

void startPage(
    PageSortedArrayAggState* state,
    TupleBuffer* parentBuffer,
    AbstractBufferProvider* bufferProvider,
    const uint64_t firstTimestamp,
    const OriginId originId,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    [[maybe_unused]] const uint64_t valueSize)
{
    PRECONDITION(state != nullptr, "ARRAY_AGG_PAGE_SORTED state must not be null");
    PRECONDITION(parentBuffer != nullptr, "ARRAY_AGG_PAGE_SORTED parent buffer must not be null");
    PRECONDITION(bufferProvider != nullptr, "ARRAY_AGG_PAGE_SORTED buffer provider must not be null");
    auto page = bufferProvider->getBufferBlocking();
    PRECONDITION(page.getBufferSize() >= sizeof(PageHeader) + valueSize, "ARRAY_AGG_PAGE_SORTED value does not fit into a page");
    auto* const pageData = reinterpret_cast<int8_t*>(page.getAvailableMemoryArea<>().data()); /// NOLINT
    /// NOLINTNEXTLINE(cppcoreguidelines-owning-memory) - placement new constructs storage owned by the tuple buffer
    auto* const header = new (pageData)
        PageHeader{.firstTimestamp = firstTimestamp, .numberOfValues = 0}; /// NOLINT(cppcoreguidelines-owning-memory) - placement new
    state->currentPageData = pageData;
    state->currentPageHeader = header;
    state->writeOffset = sizeof(PageHeader);
    state->pageSize = page.getBufferSize();
    state->currentOriginId = originId.getRawValue();
    state->currentSequenceNumber = sequenceNumber.getRawValue();
    state->currentChunkNumber = chunkNumber.getRawValue();
    state->hasCurrentInputBuffer = true;
    auto pages = parentBuffer->loadChildBuffer(ChildBufferIndex{state->pagesIndex});
    std::ignore = pages.storeChildBuffer(page);
}

uint64_t getNumberOfValues(const PageSortedArrayAggState* state, const TupleBuffer* parentBuffer)
{
    PRECONDITION(state != nullptr, "ARRAY_AGG_PAGE_SORTED state must not be null");
    PRECONDITION(parentBuffer != nullptr, "ARRAY_AGG_PAGE_SORTED parent buffer must not be null");
    const auto pages = parentBuffer->loadChildBuffer(ChildBufferIndex{state->pagesIndex});
    uint64_t numberOfValues = 0;
    for (uint64_t pageIndex = 0; pageIndex < pages.getNumberOfChildBuffers(); ++pageIndex)
    {
        const auto page = pages.loadChildBuffer(ChildBufferIndex{static_cast<ChildBufferIndex::Underlying>(pageIndex)});
        numberOfValues += page.getAvailableMemoryArea<PageHeader>().front().numberOfValues;
    }
    return numberOfValues;
}

void copySortedPages(const PageSortedArrayAggState* state, const TupleBuffer* parentBuffer, int8_t* destination, const uint64_t valueSize)
{
    PRECONDITION(state != nullptr, "ARRAY_AGG_PAGE_SORTED state must not be null");
    PRECONDITION(parentBuffer != nullptr, "ARRAY_AGG_PAGE_SORTED parent buffer must not be null");
    const auto rootPages = parentBuffer->loadChildBuffer(ChildBufferIndex{state->pagesIndex});
    std::vector<TupleBuffer> pages;
    pages.reserve(rootPages.getNumberOfChildBuffers());
    for (uint64_t pageIndex = 0; pageIndex < rootPages.getNumberOfChildBuffers(); ++pageIndex)
    {
        pages.emplace_back(rootPages.loadChildBuffer(ChildBufferIndex{static_cast<ChildBufferIndex::Underlying>(pageIndex)}));
    }
    std::ranges::stable_sort(
        pages, {}, [](const TupleBuffer& page) { return page.getAvailableMemoryArea<PageHeader>().front().firstTimestamp; });
    for (const auto& page : pages)
    {
        const auto& header = page.getAvailableMemoryArea<PageHeader>().front();
        const auto bytes = header.numberOfValues * valueSize;
        std::memcpy(destination, page.getAvailableMemoryArea<>().data() + sizeof(PageHeader), bytes);
        destination += bytes;
    }
}
}

ArrayAggPageSortedAggregationPhysicalFunction::ArrayAggPageSortedAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
    PRECONDITION(this->inputType.type != DataType::Type::VARSIZED, "ARRAY_AGG_PAGE_SORTED requires fixed-size input");
}

void ArrayAggPageSortedAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record,
    const nautilus::val<Timestamp>& timestamp,
    const AggregationInputBuffer& inputBuffer)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (inputType.nullable and value.isNull())
    {
        return;
    }

    const auto state = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto valueSize = nautilus::val<uint64_t>{DataTypeProvider::provideDataType(inputType.type).getSizeInBytesWithNull()};
    const auto hasCurrentInputBuffer = readValueFromMemRef<bool>(getMemberRef(state, &PageSortedArrayAggState::hasCurrentInputBuffer));
    const auto writeOffset = readValueFromMemRef<uint64_t>(getMemberRef(state, &PageSortedArrayAggState::writeOffset));
    const auto pageSize = readValueFromMemRef<uint64_t>(getMemberRef(state, &PageSortedArrayAggState::pageSize));
    const auto currentOriginId = readValueFromMemRef<uint64_t>(getMemberRef(state, &PageSortedArrayAggState::currentOriginId));
    const auto currentSequenceNumber = readValueFromMemRef<uint64_t>(getMemberRef(state, &PageSortedArrayAggState::currentSequenceNumber));
    const auto currentChunkNumber = readValueFromMemRef<uint64_t>(getMemberRef(state, &PageSortedArrayAggState::currentChunkNumber));
    if (not hasCurrentInputBuffer || currentOriginId != inputBuffer.originId || currentSequenceNumber != inputBuffer.sequenceNumber
        || currentChunkNumber != inputBuffer.chunkNumber || writeOffset + valueSize > pageSize)
    {
        nautilus::invoke(
            startPage,
            static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState),
            parentBuffer,
            pipelineMemoryProvider.bufferProvider,
            timestamp.convertToValue(),
            inputBuffer.originId,
            inputBuffer.sequenceNumber,
            inputBuffer.chunkNumber,
            valueSize);
    }

    const auto currentPageData = readValueFromMemRef<int8_t*>(getMemberRef(state, &PageSortedArrayAggState::currentPageData));
    const auto currentPageHeader = readValueFromMemRef<PageHeader*>(getMemberRef(state, &PageSortedArrayAggState::currentPageHeader));
    const auto currentWriteOffset = readValueFromMemRef<uint64_t>(getMemberRef(state, &PageSortedArrayAggState::writeOffset));
    value.writeToMemory(currentPageData + currentWriteOffset);
    VarVal{currentWriteOffset + valueSize}.writeToMemory(getMemberRef(state, &PageSortedArrayAggState::writeOffset));
    const auto numberOfValues
        = readValueFromMemRef<uint64_t>(getMemberRef(static_cast<nautilus::val<int8_t*>>(currentPageHeader), &PageHeader::numberOfValues));
    VarVal{numberOfValues + 1}.writeToMemory(
        getMemberRef(static_cast<nautilus::val<int8_t*>>(currentPageHeader), &PageHeader::numberOfValues));
}

void ArrayAggPageSortedAggregationPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<TupleBuffer*> parentBuffer1,
    nautilus::val<AggregationState*> aggregationState2,
    nautilus::val<TupleBuffer*> parentBuffer2,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](PageSortedArrayAggState* destination,
            TupleBuffer* destinationParent,
            const PageSortedArrayAggState* source,
            const TupleBuffer* sourceParent)
        {
            auto destinationPages = destinationParent->loadChildBuffer(ChildBufferIndex{destination->pagesIndex});
            const auto sourcePages = sourceParent->loadChildBuffer(ChildBufferIndex{source->pagesIndex});
            for (uint64_t pageIndex = 0; pageIndex < sourcePages.getNumberOfChildBuffers(); ++pageIndex)
            {
                auto page = sourcePages.loadChildBuffer(ChildBufferIndex{static_cast<ChildBufferIndex::Underlying>(pageIndex)});
                std::ignore = destinationPages.storeChildBuffer(page);
            }
        },
        static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState1),
        parentBuffer1,
        static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState2),
        parentBuffer2);
}

Record ArrayAggPageSortedAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto state = static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState);
    const auto numberOfValues = nautilus::invoke(getNumberOfValues, state, parentBuffer);
    const auto valueSize = nautilus::val<uint64_t>{DataTypeProvider::provideDataType(inputType.type).getSizeInBytesWithNull()};
    auto payload = pipelineMemoryProvider.arena.allocateVariableSizedData(numberOfValues * valueSize);
    nautilus::invoke(copySortedPages, state, parentBuffer, payload.getContent(), valueSize);
    Record result;
    result.write(resultFieldIdentifier, VarVal{payload});
    return result;
}

void ArrayAggPageSortedAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    nautilus::invoke(
        +[](PageSortedArrayAggState* state, TupleBuffer* parent, AbstractBufferProvider* bufferProvider)
        {
            auto root = bufferProvider->getUnpooledBuffer(1);
            if (not root)
            {
                throw BufferAllocationFailure("No unpooled TupleBuffer available for ARRAY_AGG_PAGE_SORTED root");
            }
            const auto rootIndex = parent->storeChildBuffer(*root).getRawValue();
            new (state) PageSortedArrayAggState{.pagesIndex = rootIndex};
        },
        static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState),
        parentBuffer,
        pipelineMemoryProvider.bufferProvider);
}

void ArrayAggPageSortedAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
    /// The pages root is a child of the parent hash-map buffer and is released with its parent.
}

size_t ArrayAggPageSortedAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(PageSortedArrayAggState);
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterArrayAggPageSortedAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<ArrayAggPageSortedAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        std::move(arguments.inputFunction),
        std::move(arguments.resultFieldIdentifier));
}
}
