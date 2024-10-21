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

namespace NES::Nautilus::Interface
{

uint64_t getTotalNumberOfEntriesProxy(const PagedVector* pagedVector)
{
    return pagedVector->getTotalNumberOfEntries();
}

void allocateNewPageProxy(PagedVector* pagedVector)
{
    pagedVector->appendPage();
}

Memory::TupleBuffer* getLastPageProxy(PagedVector* pagedVector, const uint64_t entryPos)
{
    auto lastPage = pagedVector->getPages().back();
    lastPage.setNumberOfTuples(lastPage.getNumberOfTuples() + 1);
    return pagedVector->getTupleBufferForEntry(entryPos);
}

Memory::TupleBuffer* getTupleBufferForEntryProxy(PagedVector* pagedVector, const uint64_t entryPos)
{
    return pagedVector->getTupleBufferForEntry(entryPos);
}

uint64_t getBufferPosForEntryProxy(PagedVector* pagedVector, const uint64_t entryPos)
{
    return pagedVector->getBufferPosForEntry(entryPos);
}

PagedVectorRef::PagedVectorRef(const nautilus::val<PagedVector*>& pagedVectorRef, Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout)
    : pagedVectorRef(pagedVectorRef)
    , memoryLayout(std::move(memoryLayout))
    , totalNumberOfEntries(invoke(getTotalNumberOfEntriesProxy, pagedVectorRef))
{
}

void PagedVectorRef::writeRecord(const Record& record)
{
    auto recordBuffer = RecordBuffer(invoke(getLastPageProxy, pagedVectorRef, totalNumberOfEntries));

    auto numTuplesOnPage = recordBuffer.getNumRecords();
    if (numTuplesOnPage >= memoryLayout->getCapacity())
    {
        invoke(allocateNewPageProxy, pagedVectorRef);
        numTuplesOnPage = 0;
    }

    recordBuffer.setNumRecords(numTuplesOnPage + 1);
    totalNumberOfEntries += 1;

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(memoryLayout->getBufferSize(), memoryLayout->getSchema());
    memoryProvider->writeRecord(numTuplesOnPage, recordBuffer, record);
}

Record PagedVectorRef::readRecord(const nautilus::val<uint64_t>& pos) const
{
    auto recordBuffer = RecordBuffer(invoke(getTupleBufferForEntryProxy, pagedVectorRef, pos));
    auto recordEntry = invoke(getBufferPosForEntryProxy, pagedVectorRef, pos);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(memoryLayout->getBufferSize(), memoryLayout->getSchema());
    return memoryProvider->readRecord(memoryLayout->getSchema()->getFieldNames(), recordBuffer, recordEntry);
}

Record PagedVectorRef::readRecordKeys(const nautilus::val<uint64_t>& pos) const
{
    auto recordBuffer = RecordBuffer(invoke(getTupleBufferForEntryProxy, pagedVectorRef, pos));
    auto recordEntry = invoke(getBufferPosForEntryProxy, pagedVectorRef, pos);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(memoryLayout->getBufferSize(), memoryLayout->getSchema());
    return memoryProvider->readRecord(memoryLayout->getKeyFieldNames(), recordBuffer, recordEntry);
}

PagedVectorRefIter PagedVectorRef::begin() const
{
    return at(0);
}

PagedVectorRefIter PagedVectorRef::at(const nautilus::val<uint64_t>& pos) const
{
    PagedVectorRefIter pagedVectorRefIter(*this);
    pagedVectorRefIter.setPos(pos);
    return pagedVectorRefIter;
}

PagedVectorRefIter PagedVectorRef::end() const
{
    return at(totalNumberOfEntries);
}

bool PagedVectorRef::operator==(const PagedVectorRef& other) const
{
    if (this == &other)
    {
        return true;
    }

    return memoryLayout == other.memoryLayout && pagedVectorRef == other.pagedVectorRef
        && totalNumberOfEntries == other.totalNumberOfEntries;
}

PagedVectorRefIter::PagedVectorRefIter(const PagedVectorRef& pagedVector) : pos(0), pagedVector(pagedVector)
{
}

Record PagedVectorRefIter::operator*() const
{
    return pagedVector.readRecord(pos);
}

PagedVectorRefIter& PagedVectorRefIter::operator++()
{
    pos = pos + 1;
    return *this;
}

bool PagedVectorRefIter::operator==(const PagedVectorRefIter& other) const
{
    if (this == &other)
    {
        return true;
    }

    return pos == other.pos && pagedVector == other.pagedVector;
}

bool PagedVectorRefIter::operator!=(const PagedVectorRefIter& other) const
{
    return !(*this == other);
}

void PagedVectorRefIter::setPos(const nautilus::val<uint64_t>& newPos)
{
    pos = newPos;
}

}
