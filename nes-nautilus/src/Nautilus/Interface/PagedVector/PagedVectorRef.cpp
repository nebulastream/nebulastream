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
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::Interface
{

uint64_t getTotalNumberOfEntriesProxy(const PagedVector* pagedVector)
{
    return pagedVector->getTotalNumberOfEntries();
}

const TupleBuffer* createNewEntryProxy(PagedVector* pagedVector, AbstractBufferProvider* bufferProvider, const MemoryLayout* memoryLayout)
{
    pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
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

PagedVectorRef::PagedVectorRef(
    const nautilus::val<PagedVector*>& pagedVectorRef, const std::shared_ptr<BufferRef::TupleBufferRef>& bufferRef)
    : pagedVectorRef(pagedVectorRef), bufferRef(bufferRef), memoryLayout(bufferRef->getMemoryLayout().get())
{
}

void PagedVectorRef::writeRecord(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    auto recordBuffer = RecordBuffer(invoke(createNewEntryProxy, pagedVectorRef, bufferProvider, memoryLayout));
    auto numTuplesOnPage = recordBuffer.getNumRecords();
    bufferRef->writeRecord(numTuplesOnPage, recordBuffer, record, bufferProvider);
    recordBuffer.setNumRecords(numTuplesOnPage + 1);
}

Record PagedVectorRef::readRecord(const nautilus::val<uint64_t>& pos, const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    /// As we can not return two values via one invoke, we have to perform two invokes
    /// This is still less than iterating over the pages in the PagedVector here and calling getNumberOfTuples on each page.
    /// As calling getNumberOfTuples on each page would require one invoke per page.
    const auto recordBuffer = RecordBuffer(invoke(getTupleBufferForEntryProxy, pagedVectorRef, pos));
    auto recordEntry = invoke(getBufferPosForEntryProxy, pagedVectorRef, pos);
    const auto record = bufferRef->readRecord(projections, recordBuffer, recordEntry);
    return record;
}

PagedVectorRefIter PagedVectorRef::begin(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    const nautilus::val<uint64_t> pos(0);
    const auto numberOfTuplesInPagedVector = invoke(getTotalNumberOfEntriesProxy, pagedVectorRef);
    const auto curPage = nautilus::invoke(getTupleBufferForEntryProxy, pagedVectorRef, pos);
    const auto posOnPage = nautilus::invoke(getBufferPosForEntryProxy, pagedVectorRef, pos);
    const auto tuplesOnPage = RecordBuffer(curPage).getNumRecords();
    PagedVectorRefIter pagedVectorRefIter(*this, bufferRef, projections, curPage, posOnPage, tuplesOnPage, numberOfTuplesInPagedVector);
    return pagedVectorRefIter;
}

PagedVectorRefIter PagedVectorRef::end(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    /// End does not point to any existing page. Therefore, we only set the pos
    const auto pos = invoke(getTotalNumberOfEntriesProxy, pagedVectorRef);
    const nautilus::val<TupleBuffer*> curPage(nullptr);
    PagedVectorRefIter pagedVectorRefIter(*this, bufferRef, projections, curPage, pos, pos, pos);
    return pagedVectorRefIter;
}

nautilus::val<bool> PagedVectorRef::operator==(const PagedVectorRef& other) const
{
    return bufferRef == other.bufferRef && pagedVectorRef == other.pagedVectorRef;
}

PagedVectorRefIter::PagedVectorRefIter(
    PagedVectorRef pagedVector,
    const std::shared_ptr<BufferRef::TupleBufferRef>& bufferRef,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<TupleBuffer*>& curPage,
    const nautilus::val<uint64_t>& posOnPage,
    const nautilus::val<uint64_t>& numberOfTuplesOnPage,
    const nautilus::val<uint64_t>& numberOfTuplesInPagedVector)
    : pagedVector(std::move(pagedVector))
    , posOnPageRollOver(RolloverIndex<uint64_t>{
          posOnPage,
          numberOfTuplesOnPage,
          [&](const nautilus::val<uint64_t>& oldRolloverValue, const nautilus::val<uint64_t>& overallValue)
          {
              /// Go to the next page
              if (overallValue < this->numberOfTuplesInPagedVector)
              {
                  this->curPage = nautilus::invoke(getTupleBufferForEntryProxy, this->pagedVector.pagedVectorRef, overallValue);
                  return RecordBuffer(this->curPage).getNumRecords();
              }
              return oldRolloverValue;
          }})
    , projections(projections)
    , numberOfTuplesInPagedVector(numberOfTuplesInPagedVector)
    , curPage(curPage)
    , bufferRef(bufferRef)
{
}

Record PagedVectorRefIter::operator*() const
{
    const RecordBuffer recordBuffer(curPage);
    auto recordEntry = posOnPageRollOver.getCurrentIndex();
    return bufferRef->readRecord(projections, recordBuffer, recordEntry);
}

PagedVectorRefIter& PagedVectorRefIter::operator++()
{
    posOnPageRollOver.increment();
    return *this;
}

nautilus::val<bool> PagedVectorRefIter::operator==(const PagedVectorRefIter& other) const
{
    if (this == &other)
    {
        return true;
    }

    const auto samePos = posOnPageRollOver.getOverallIndex() == other.posOnPageRollOver.getOverallIndex();
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
    return posOnPageRollOver.getOverallIndex() - other.posOnPageRollOver.getOverallIndex();
}

}
