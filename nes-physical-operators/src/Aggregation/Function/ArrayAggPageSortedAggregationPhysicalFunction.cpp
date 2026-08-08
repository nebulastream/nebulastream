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
#include <numeric>
#include <ranges>
#include <utility>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <nautilus/function.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

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
    TupleBuffer pages;
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
    AbstractBufferProvider* bufferProvider,
    const uint64_t firstTimestamp,
    const OriginId originId,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    [[maybe_unused]] const uint64_t valueSize)
{
    PRECONDITION(state != nullptr, "ARRAY_AGG_PAGE_SORTED state must not be null");
    PRECONDITION(bufferProvider != nullptr, "ARRAY_AGG_PAGE_SORTED buffer provider must not be null");
    auto page = bufferProvider->getBufferBlocking();
    PRECONDITION(page.getBufferSize() >= sizeof(PageHeader) + valueSize, "ARRAY_AGG_PAGE_SORTED value does not fit into a page");
    auto* const pageData = reinterpret_cast<int8_t*>(page.getAvailableMemoryArea<>().data()); /// NOLINT
    auto* const header = new (pageData) PageHeader{firstTimestamp, 0};
    state->currentPageData = pageData;
    state->currentPageHeader = header;
    state->writeOffset = sizeof(PageHeader);
    state->pageSize = page.getBufferSize();
    state->currentOriginId = originId.getRawValue();
    state->currentSequenceNumber = sequenceNumber.getRawValue();
    state->currentChunkNumber = chunkNumber.getRawValue();
    state->hasCurrentInputBuffer = true;
    std::ignore = state->pages.storeChildBuffer(page);
}

uint64_t getNumberOfValues(const PageSortedArrayAggState* state)
{
    PRECONDITION(state != nullptr, "ARRAY_AGG_PAGE_SORTED state must not be null");
    uint64_t numberOfValues = 0;
    for (uint64_t pageIndex = 0; pageIndex < state->pages.getNumberOfChildBuffers(); ++pageIndex)
    {
        const auto page = state->pages.loadChildBuffer(VariableSizedAccess::Index{pageIndex});
        numberOfValues += page.getAvailableMemoryArea<PageHeader>().front().numberOfValues;
    }
    return numberOfValues;
}

void copySortedPages(const PageSortedArrayAggState* state, int8_t* destination, const uint64_t valueSize)
{
    PRECONDITION(state != nullptr, "ARRAY_AGG_PAGE_SORTED state must not be null");
    std::vector<TupleBuffer> pages;
    pages.reserve(state->pages.getNumberOfChildBuffers());
    for (uint64_t pageIndex = 0; pageIndex < state->pages.getNumberOfChildBuffers(); ++pageIndex)
    {
        pages.emplace_back(state->pages.loadChildBuffer(VariableSizedAccess::Index{pageIndex}));
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
    nautilus::val<AggregationState*> aggregationState1, nautilus::val<AggregationState*> aggregationState2, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](PageSortedArrayAggState* destination, const PageSortedArrayAggState* source)
        {
            for (uint64_t pageIndex = 0; pageIndex < source->pages.getNumberOfChildBuffers(); ++pageIndex)
            {
                auto page = source->pages.loadChildBuffer(VariableSizedAccess::Index{pageIndex});
                std::ignore = destination->pages.storeChildBuffer(page);
            }
        },
        static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState1),
        static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState2));
}

Record ArrayAggPageSortedAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto state = static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState);
    const auto numberOfValues = nautilus::invoke(getNumberOfValues, state);
    const auto valueSize = nautilus::val<uint64_t>{DataTypeProvider::provideDataType(inputType.type).getSizeInBytesWithNull()};
    auto payload = pipelineMemoryProvider.arena.allocateVariableSizedData(numberOfValues * valueSize);
    nautilus::invoke(copySortedPages, state, payload.getContent(), valueSize);
    Record result;
    result.write(resultFieldIdentifier, VarVal{payload});
    return result;
}

void ArrayAggPageSortedAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    nautilus::invoke(
        +[](PageSortedArrayAggState* state, AbstractBufferProvider* bufferProvider)
        {
            auto root = bufferProvider->getUnpooledBuffer(1);
            if (not root)
            {
                throw BufferAllocationFailure("No unpooled TupleBuffer available for ARRAY_AGG_PAGE_SORTED root");
            }
            new (state) PageSortedArrayAggState{.pages = std::move(*root)};
        },
        static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState),
        pipelineMemoryProvider.bufferProvider);
}

void ArrayAggPageSortedAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](PageSortedArrayAggState* state) { state->~PageSortedArrayAggState(); },
        static_cast<nautilus::val<PageSortedArrayAggState*>>(aggregationState));
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
