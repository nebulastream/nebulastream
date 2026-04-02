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
#include <Nautilus/Interface/BufferRef/BufferLayoutRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <val_ptr.hpp>

namespace NES
{

uint64_t getTotalNumberOfEntriesProxy(const PagedVector* pagedVector)
{
    return pagedVector->getTotalNumberOfEntries();
}

const static TupleBuffer*
createNewEntryProxy(PagedVector* pagedVector, AbstractBufferProvider* bufferProvider, const uint64_t capacity, const uint64_t bufferSize)
{
    pagedVector->appendPageIfFull(bufferProvider, capacity, bufferSize);
    return std::addressof(pagedVector->getLastPage());
}

const TupleBuffer* getFirstPageProxy(const PagedVector* pagedVector)
{
    return std::addressof(pagedVector->getFirstPage());
}

const TupleBuffer* getTupleBufferForEntryProxy(const PagedVector* pagedVector, const uint64_t entryPos)
{
    return pagedVector->getTupleBufferForEntry(entryPos);
}

uint64_t getBufferPosForEntryProxy(const PagedVector* pagedVector, const uint64_t entryPos)
{
    return pagedVector->getBufferPosForEntry(entryPos).value_or(0);
}

nautilus::val<uint64_t> PagedVectorRef::getNumberOfTuples() const
{
    return nautilus::invoke(getTotalNumberOfEntriesProxy, pagedVectorRef);
}

PagedVectorRef::PagedVectorRef(const nautilus::val<PagedVector*>& pagedVectorRef, const std::shared_ptr<BufferLayoutRef>& layout)
    : pagedVectorRef(pagedVectorRef), pageLayout(layout)
{
}

void PagedVectorRef::writeRecord(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    auto recordBuffer = RecordBuffer(invoke(
        createNewEntryProxy,
        pagedVectorRef,
        bufferProvider,
        nautilus::val<uint64_t>{pageLayout->getCapacity()},
        nautilus::val<uint64_t>{pageLayout->getBufferSize()}));
    auto numTuplesOnPage = recordBuffer.getNumRecords();
    pageLayout->writeRecord(numTuplesOnPage, recordBuffer, record, bufferProvider);
    recordBuffer.setNumRecords(numTuplesOnPage + 1);
}


Record PagedVectorRef::readRecord(const nautilus::val<uint64_t>& pos, const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    const auto recordBuffer = RecordBuffer(invoke(getTupleBufferForEntryProxy, pagedVectorRef, pos));
    auto recordEntry = invoke(getBufferPosForEntryProxy, pagedVectorRef, pos);
    return pageLayout->readRecord(projections, recordBuffer, recordEntry);
}

PagedVectorRefIter PagedVectorRef::begin(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    const nautilus::val<uint64_t> pos(0);
    const auto numberOfTuplesInPagedVector = invoke(getTotalNumberOfEntriesProxy, pagedVectorRef);
    const auto curPage = nautilus::invoke(getTupleBufferForEntryProxy, pagedVectorRef, pos);
    const auto posOnPage = nautilus::invoke(getBufferPosForEntryProxy, pagedVectorRef, pos);
    return PagedVectorRefIter(*this, pageLayout, projections, curPage, posOnPage, pos, numberOfTuplesInPagedVector);
}


PagedVectorRefIter PagedVectorRef::end(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    const auto pos = invoke(getTotalNumberOfEntriesProxy, pagedVectorRef);
    const nautilus::val<TupleBuffer*> curPage(nullptr);
    const nautilus::val<uint64_t> posOnPage(0);
    return PagedVectorRefIter(*this, pageLayout, projections, curPage, posOnPage, pos, pos);
}

nautilus::val<bool> PagedVectorRef::operator==(const PagedVectorRef& other) const
{
    return pageLayout == other.pageLayout && pagedVectorRef == other.pagedVectorRef;
}

PagedVectorRefIter::PagedVectorRefIter(
    PagedVectorRef pagedVector,
    const std::shared_ptr<BufferLayoutRef>& pageLayout,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<TupleBuffer*>& curPage,
    const nautilus::val<uint64_t>& posOnPage,
    const nautilus::val<uint64_t>& pos,
    const nautilus::val<uint64_t>& numberOfTuplesInPagedVector)
    : pagedVector(std::move(pagedVector))
    , projections(projections)
    , pos(pos)
    , numberOfTuplesInPagedVector(numberOfTuplesInPagedVector)
    , posOnPage(posOnPage)
    , curPage(curPage)
    , pageLayout(pageLayout)
{
}

Record PagedVectorRefIter::operator*() const
{
    const RecordBuffer recordBuffer(curPage);
    auto recordEntry = posOnPage;
    return pageLayout->readRecord(projections, recordBuffer, recordEntry);
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
            curPage = nautilus::invoke(getTupleBufferForEntryProxy, this->pagedVector.pagedVectorRef, this->pos);
            posOnPage = nautilus::invoke(getBufferPosForEntryProxy, this->pagedVector.pagedVectorRef, this->pos);
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
