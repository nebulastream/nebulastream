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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::Interface
{

uint64_t getTotalNumberOfEntriesProxy(const PagedVector* pagedVector)
{
    return pagedVector->getTotalNumberOfEntries();
}

const Memory::PinnedBuffer* createNewEntryProxy(
    PagedVector* pagedVector, Memory::AbstractBufferProvider* bufferProvider, const Memory::MemoryLayouts::MemoryLayout* memoryLayout)
{
    pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
    return std::addressof(pagedVector->getLastPage());
}

const Memory::PinnedBuffer* getFirstPageProxy(const PagedVector* pagedVector)
{
    return std::addressof(pagedVector->getFirstPage());
}

const Memory::PinnedBuffer* getTupleBufferForEntryProxy(const PagedVector* pagedVector, const uint64_t entryPos)
{
    return std::addressof(pagedVector->getTupleBufferForEntry(entryPos));
}

uint64_t getBufferPosForEntryProxy(const PagedVector* pagedVector, const uint64_t entryPos)
{
    return pagedVector->getBufferPosForEntry(entryPos);
}

PagedVectorRef::PagedVectorRef(
    const nautilus::val<PagedVector*>& pagedVectorRef,
    const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
    const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
    : pagedVectorRef(pagedVectorRef)
    , memoryProvider(memoryProvider)
    , memoryLayout(memoryProvider->getMemoryLayout().get())
    , bufferProvider(bufferProvider)
{
}

void PagedVectorRef::writeRecord(const Record& record) const
{
    auto recordBuffer = RecordBuffer(invoke(createNewEntryProxy, pagedVectorRef, bufferProvider, memoryLayout));
    auto numTuplesOnPage = recordBuffer.getNumRecords();
    memoryProvider->writeRecord(numTuplesOnPage, recordBuffer, record);
    recordBuffer.setNumRecords(numTuplesOnPage + 1);
}

Record PagedVectorRef::readRecord(const nautilus::val<uint64_t>& pos, const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    /// As we can not return two values via one invoke, we have to perform two invokes
    /// This is still less than iterating over the pages in the PagedVector here and calling getNumberOfTuples on each page.
    /// As calling getNumberOfTuples on each page would require one invoke per page.
    const auto recordBuffer = RecordBuffer(invoke(getTupleBufferForEntryProxy, pagedVectorRef, pos));
    auto recordEntry = invoke(getBufferPosForEntryProxy, pagedVectorRef, pos);
    const auto record = memoryProvider->readRecord(projections, recordBuffer, recordEntry);
    return record;
}

PagedVectorRefIter PagedVectorRef::begin(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    return at(projections, 0);
}

PagedVectorRefIter
PagedVectorRef::at(const std::vector<Record::RecordFieldIdentifier>& projections, const nautilus::val<uint64_t>& pos) const
{
    PagedVectorRefIter pagedVectorRefIter(*this, projections, pos);
    return pagedVectorRefIter;
}

PagedVectorRefIter PagedVectorRef::end(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    const auto pos = invoke(getTotalNumberOfEntriesProxy, pagedVectorRef);
    return at(projections, pos);
}

nautilus::val<bool> PagedVectorRef::operator==(const PagedVectorRef& other) const
{
    return memoryProvider == other.memoryProvider && pagedVectorRef == other.pagedVectorRef;
}

PagedVectorRefIter::PagedVectorRefIter(
    PagedVectorRef pagedVector, const std::vector<Record::RecordFieldIdentifier>& projections, const nautilus::val<uint64_t>& pos)
    : pagedVector(std::move(pagedVector)), projections(projections), pos(pos)
{
}

Record PagedVectorRefIter::operator*() const
{
    return pagedVector.readRecord(pos, projections);
}

PagedVectorRefIter& PagedVectorRefIter::operator++()
{
    pos = pos + 1;
    return *this;
}

nautilus::val<bool> PagedVectorRefIter::operator==(const PagedVectorRefIter& other) const
{
    if (this == &other)
    {
        return true;
    }

    const auto samePos = pos == other.pos;
    const auto samePagedVector = pagedVector == other.pagedVector;
    const auto sameProjections = projections == other.projections;
    return samePos && samePagedVector && sameProjections;
}

nautilus::val<bool> PagedVectorRefIter::operator!=(const PagedVectorRefIter& other) const
{
    return !(*this == other);
}

nautilus::val<uint64_t> PagedVectorRefIter::operator-(const PagedVectorRefIter& other) const
{
    return pos - other.pos;
}

}
