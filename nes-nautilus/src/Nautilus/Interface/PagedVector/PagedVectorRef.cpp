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

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <val_ptr.hpp>

namespace NES
{

uint64_t getTotalNumberOfEntriesProxy(const TupleBuffer* pagedVectorBuffer)
{
    PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
    return pagedVector.getTotalNumberOfEntries();
}

uint64_t getBufferPosForEntryProxy(const TupleBuffer* pagedVectorBuffer, const uint64_t entryPos)
{
    PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
    return pagedVector.getBufferPosForEntry(entryPos).value_or(0);
}

nautilus::val<uint64_t> PagedVectorRef::getNumberOfTuples() const
{
    return nautilus::invoke(getTotalNumberOfEntriesProxy, pagedVectorBuffer);
}

PagedVectorRef::PagedVectorRef(const nautilus::val<TupleBuffer*>& pagedVectorBuffer, const std::shared_ptr<TupleBufferRef>& bufferRef)
    : pagedVectorBuffer(pagedVectorBuffer), bufferRef(bufferRef)
{
}

void PagedVectorRef::writeRecord(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    auto lastPage = invoke(
        +[](TupleBuffer* pagedVectorBuffer, AbstractBufferProvider* bufferProvider, const uint64_t capacity, const uint64_t bufferSize)
            -> TupleBuffer*
        {
            PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            pagedVector.appendPageIfFull(bufferProvider, capacity, bufferSize);
            auto numPages = pagedVector.getNumberOfPages();
            VariableSizedAccess::Index lastPageIndex{numPages - 1};
            return std::addressof(pagedVectorBuffer->getChildRef(lastPageIndex));
        },
        pagedVectorBuffer,
        bufferProvider,
        nautilus::val<uint64_t>{bufferRef->getCapacity()},
        nautilus::val<uint64_t>{bufferRef->getBufferSize()});

    auto recordBuffer = RecordBuffer(lastPage);
    auto numTuplesOnPage = recordBuffer.getNumRecords();
    bufferRef->writeRecord(numTuplesOnPage, recordBuffer, record, bufferProvider, PagedVector::Page::getHeaderSize());
    recordBuffer.setNumRecords(numTuplesOnPage + 1);
}

Record PagedVectorRef::readRecord(const nautilus::val<uint64_t>& pos, const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    /// As we can not return two values via one invoke, we have to perform two invokes
    /// This is still less than iterating over the pages in the PagedVector here and calling getNumberOfTuples on each page.
    /// As calling getNumberOfTuples on each page would require one invoke per page.
    auto curPage = invoke(
        +[](TupleBuffer* pagedVectorBuffer, uint64_t entryPos) -> TupleBuffer*
        {
            PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            if (const auto index = pagedVector.findIdx(entryPos); index.has_value())
            {
                VariableSizedAccess::Index childBufferIndex{index.value()};
                return std::addressof(pagedVectorBuffer->getChildRef(childBufferIndex));
            }
            return nullptr;
        },
        pagedVectorBuffer,
        pos);
    const auto recordBuffer = RecordBuffer(curPage);
    auto recordEntry = invoke(getBufferPosForEntryProxy, pagedVectorBuffer, pos);
    const auto record = bufferRef->readRecord(projections, recordBuffer, recordEntry, PagedVector::Page::getHeaderSize());
    return record;
}

PagedVectorRefIter PagedVectorRef::begin(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    const nautilus::val<uint64_t> pos(0);
    const auto numberOfTuplesInPagedVector = invoke(getTotalNumberOfEntriesProxy, pagedVectorBuffer);
    auto curPage = invoke(
        +[](TupleBuffer* pagedVectorBuffer, uint64_t entryPos) -> TupleBuffer*
        {
            PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            if (const auto index = pagedVector.findIdx(entryPos); index.has_value())
            {
                VariableSizedAccess::Index childBufferIndex{index.value()};
                return std::addressof(pagedVectorBuffer->getChildRef(childBufferIndex));
            }
            return nullptr;
        },
        pagedVectorBuffer,
        pos);
    const auto posOnPage = nautilus::invoke(getBufferPosForEntryProxy, pagedVectorBuffer, pos);
    PagedVectorRefIter pagedVectorRefIter(*this, bufferRef, projections, curPage, posOnPage, pos, numberOfTuplesInPagedVector);
    return pagedVectorRefIter;
}

PagedVectorRefIter PagedVectorRef::end(const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    /// End does not point to any existing page. Therefore, we only set the pos
    const auto pos = invoke(getTotalNumberOfEntriesProxy, pagedVectorBuffer);
    const nautilus::val<TupleBuffer*> curPage(nullptr);
    const nautilus::val<uint64_t> posOnPage(0);
    PagedVectorRefIter pagedVectorRefIter(*this, bufferRef, projections, curPage, posOnPage, pos, pos);
    return pagedVectorRefIter;
}

nautilus::val<bool> PagedVectorRef::operator==(const PagedVectorRef& other) const
{
    return bufferRef == other.bufferRef && pagedVectorBuffer == other.pagedVectorBuffer;
}

PagedVectorRefIter::PagedVectorRefIter(
    PagedVectorRef pagedVector,
    const std::shared_ptr<TupleBufferRef>& bufferRef,
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
    , bufferRef(bufferRef)
{
}

Record PagedVectorRefIter::operator*() const
{
    const RecordBuffer recordBuffer(curPage);
    auto recordEntry = posOnPage;
    return bufferRef->readRecord(projections, recordBuffer, recordEntry, PagedVector::Page::getHeaderSize());
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
            curPage = invoke(
                +[](TupleBuffer* pagedVectorBuffer, uint64_t entryPos) -> TupleBuffer*
                {
                    PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
                    if (const auto index = pagedVector.findIdx(entryPos); index.has_value())
                    {
                        VariableSizedAccess::Index childBufferIndex{index.value()};
                        return std::addressof(pagedVectorBuffer->getChildRef(childBufferIndex));
                    }
                    return nullptr;
                },
                this->pagedVector.pagedVectorBuffer,
                this->pos);
            posOnPage = nautilus::invoke(getBufferPosForEntryProxy, this->pagedVector.pagedVectorBuffer, this->pos);
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
