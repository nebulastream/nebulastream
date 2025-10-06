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
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>

#include <memory>
#include <utility>
#include <vector>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <val_ptr.hpp>
#include <Nautilus/DataStructures/SerializablePagedVector.hpp>

namespace NES::Nautilus::Interface
{

nautilus::val<uint64_t> PagedVectorRef::getNumberOfTuples() const
{
    return nautilus::invoke(fnGetTotalEntries, anyVectorRef);
}

PagedVectorRef::PagedVectorRef(
    const nautilus::val<PagedVector*>& pagedVectorRef,
    const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider)
    : anyVectorRef(static_cast<nautilus::val<void*>>(static_cast<nautilus::val<void*>>(pagedVectorRef)))
    , memoryProvider(memoryProvider)
    , memoryLayout(memoryProvider->getMemoryLayout().get())
{
    fnGetTotalEntries = +[](void* ptr) -> uint64_t { return static_cast<PagedVector*>(ptr)->getTotalNumberOfEntries(); };
    fnCreateNewEntry = +[](void* ptr, Memory::AbstractBufferProvider* provider, const Memory::MemoryLayouts::MemoryLayout* layout) {
        auto* vector = static_cast<PagedVector*>(ptr);
        vector->appendPageIfFull(provider, layout);
        return std::addressof(vector->getLastPage());
    };
    fnGetTupleBufferForEntry = +[](void* ptr, uint64_t entry) -> const Memory::TupleBuffer* {
        return static_cast<PagedVector*>(ptr)->getTupleBufferForEntry(entry);
    };
    fnGetBufferPosForEntry = +[](void* ptr, uint64_t entry) -> uint64_t {
        return static_cast<PagedVector*>(ptr)->getBufferPosForEntry(entry).value_or(0);
    };
}

PagedVectorRef::PagedVectorRef(
    const nautilus::val<DataStructures::SerializablePagedVector*>& pagedVectorRef,
    const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider)
    : anyVectorRef(static_cast<nautilus::val<void*>>(static_cast<nautilus::val<void*>>(pagedVectorRef)))
    , memoryProvider(memoryProvider)
    , memoryLayout(memoryProvider->getMemoryLayout().get())
{
    fnGetTotalEntries = +[](void* ptr) -> uint64_t {
        return static_cast<DataStructures::SerializablePagedVector*>(ptr)->getTotalNumberOfEntries();
    };
    fnCreateNewEntry = +[](void* ptr, Memory::AbstractBufferProvider* provider, const Memory::MemoryLayouts::MemoryLayout* layout) {
        auto* vector = static_cast<DataStructures::SerializablePagedVector*>(ptr);
        vector->appendPageIfFull(provider, layout);
        return std::addressof(vector->getLastPage());
    };
    fnGetTupleBufferForEntry = +[](void* ptr, uint64_t entry) -> const Memory::TupleBuffer* {
        return static_cast<DataStructures::SerializablePagedVector*>(ptr)->getTupleBufferForEntry(entry);
    };
    fnGetBufferPosForEntry = +[](void* ptr, uint64_t entry) -> uint64_t {
        return static_cast<DataStructures::SerializablePagedVector*>(ptr)->getBufferPosForEntry(entry).value_or(0);
    };
}

PagedVectorRef::PagedVectorRef(
    const nautilus::val<int8_t*>& valueAreaPtr,
    const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider)
    : anyVectorRef(static_cast<nautilus::val<void*>>(valueAreaPtr))
    , memoryProvider(memoryProvider)
    , memoryLayout(memoryProvider->getMemoryLayout().get())
{
    fnGetTotalEntries = +[](void* ptr) -> uint64_t { return static_cast<PagedVector*>(ptr)->getTotalNumberOfEntries(); };
    fnCreateNewEntry = +[](void* ptr, Memory::AbstractBufferProvider* provider, const Memory::MemoryLayouts::MemoryLayout* layout) {
        auto* vector = static_cast<PagedVector*>(ptr);
        vector->appendPageIfFull(provider, layout);
        return std::addressof(vector->getLastPage());
    };
    fnGetTupleBufferForEntry = +[](void* ptr, uint64_t entry) -> const Memory::TupleBuffer* {
        return static_cast<PagedVector*>(ptr)->getTupleBufferForEntry(entry);
    };
    fnGetBufferPosForEntry = +[](void* ptr, uint64_t entry) -> uint64_t {
        return static_cast<PagedVector*>(ptr)->getBufferPosForEntry(entry).value_or(0);
    };
}

void PagedVectorRef::writeRecord(const Record& record, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) const
{
    auto recordBuffer = RecordBuffer(invoke(fnCreateNewEntry, anyVectorRef, bufferProvider, memoryLayout));
    auto numTuplesOnPage = recordBuffer.getNumRecords();
    memoryProvider->writeRecord(numTuplesOnPage, recordBuffer, record, bufferProvider);
    recordBuffer.setNumRecords(numTuplesOnPage + 1);
}

Record PagedVectorRef::readRecord(const nautilus::val<uint64_t>& pos, const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    /// As we can not return two values via one invoke, we have to perform two invokes
    /// This is still less than iterating over the pages in the PagedVector here and calling getNumberOfTuples on each page.
    /// As calling getNumberOfTuples on each page would require one invoke per page.
    const auto recordBuffer = RecordBuffer(invoke(fnGetTupleBufferForEntry, anyVectorRef, pos));
    auto recordEntry = invoke(fnGetBufferPosForEntry, anyVectorRef, pos);
    const auto record = memoryProvider->readRecord(projections, recordBuffer, recordEntry);
    return record;
}

PagedVectorRefIter PagedVectorRef::begin(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    const nautilus::val<uint64_t> pos(0);
    const auto numberOfTuplesInPagedVector = invoke(fnGetTotalEntries, anyVectorRef);
    const auto curPage = nautilus::invoke(fnGetTupleBufferForEntry, anyVectorRef, pos);
    const auto posOnPage = nautilus::invoke(fnGetBufferPosForEntry, anyVectorRef, pos);
    PagedVectorRefIter pagedVectorRefIter(*this, memoryProvider, projections, curPage, posOnPage, pos, numberOfTuplesInPagedVector);
    return pagedVectorRefIter;
}

PagedVectorRefIter PagedVectorRef::end(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    /// End does not point to any existing page. Therefore, we only set the pos
    const auto pos = invoke(fnGetTotalEntries, anyVectorRef);
    const nautilus::val<Memory::TupleBuffer*> curPage(nullptr);
    const nautilus::val<uint64_t> posOnPage(0);
    PagedVectorRefIter pagedVectorRefIter(*this, memoryProvider, projections, curPage, posOnPage, pos, pos);
    return pagedVectorRefIter;
}

nautilus::val<bool> PagedVectorRef::operator==(const PagedVectorRef& other) const
{
    return memoryProvider == other.memoryProvider && anyVectorRef == other.anyVectorRef;
}

PagedVectorRefIter::PagedVectorRefIter(
    PagedVectorRef pagedVector,
    const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<Memory::TupleBuffer*>& curPage,
    const nautilus::val<uint64_t>& posOnPage,
    const nautilus::val<uint64_t>& pos,
    const nautilus::val<uint64_t>& numberOfTuplesInPagedVector)
    : pagedVector(std::move(pagedVector))
    , projections(projections)
    , pos(pos)
    , numberOfTuplesInPagedVector(numberOfTuplesInPagedVector)
    , posOnPage(posOnPage)
    , curPage(curPage)
    , memoryProvider(memoryProvider)
{
}

Record PagedVectorRefIter::operator*() const
{
    const RecordBuffer recordBuffer(curPage);
    auto recordEntry = posOnPage;
    return memoryProvider->readRecord(projections, recordBuffer, recordEntry);
}

PagedVectorRefIter& PagedVectorRefIter::operator++()
{
    pos = pos + 1;
    posOnPage = posOnPage + 1;
    const auto tuplesOnPage = RecordBuffer(curPage).getNumRecords();
    if (posOnPage >= tuplesOnPage)
    {
        /// Go to the next page
        posOnPage = 0;
        if (pos < numberOfTuplesInPagedVector)
        {
            curPage = nautilus::invoke(this->pagedVector.fnGetTupleBufferForEntry, this->pagedVector.anyVectorRef, this->pos);
            posOnPage = nautilus::invoke(this->pagedVector.fnGetBufferPosForEntry, this->pagedVector.anyVectorRef, this->pos);
        }
    }
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
