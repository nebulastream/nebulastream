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

uint64_t getNumberOfPagesProxy(PagedVector* pagedVector)
{
    return pagedVector->getPages().size();
}

Memory::TupleBuffer* getFirstPageProxy(PagedVector* pagedVector)
{
    auto& dataVec = pagedVector->getPages();
    const auto dataStart = dataVec.data();
    const auto dataStart2 = dataVec.begin();
    const auto firstItem = dataVec[0];
    auto tmpBuffer = new Memory::TupleBuffer(dataVec[0]);
    return tmpBuffer;
}

Memory::TupleBuffer* allocateNewPageProxy(PagedVector* pagedVector)
{
    return pagedVector->appendPage();
}

PagedVectorRef::PagedVectorRef(const nautilus::val<PagedVector*>& pagedVectorRef, Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout)
    : pagedVectorRef(pagedVectorRef)
    , memoryLayout(std::move(memoryLayout))
    , firstPage(invoke(getFirstPageProxy, pagedVectorRef))
    , numPages(invoke(getNumberOfPagesProxy, pagedVectorRef))
{
}

void PagedVectorRef::writeRecord(const Record& record)
{
    auto recordBuffer = RecordBuffer(firstPage + ((numPages - 1) * sizeof(Memory::TupleBuffer)));
    auto numTuplesOnPage = recordBuffer.getNumRecords();

    if (numTuplesOnPage >= memoryLayout->getCapacity())
    {
        /// pages vector is potentially moved due to resize
        firstPage = invoke(allocateNewPageProxy, pagedVectorRef);
        numPages += 1; /// as we have now added a page
        recordBuffer = RecordBuffer(firstPage + ((numPages - 1) * sizeof(Memory::TupleBuffer)));
        numTuplesOnPage = 0;
    }

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(memoryLayout->getBufferSize(), memoryLayout->getSchema());
    memoryProvider->writeRecord(numTuplesOnPage, recordBuffer, record);
    recordBuffer.setNumRecords(numTuplesOnPage + 1);
}

Record PagedVectorRef::readRecord(const nautilus::val<uint64_t>& pos) const
{
    auto [recordEntry, recordBufferAddr] = getTupleBufferAndPosForEntry(pos);
    auto recordBuffer = RecordBuffer(recordBufferAddr);

    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(memoryLayout->getBufferSize(), memoryLayout->getSchema());
    return memoryProvider->readRecord(memoryLayout->getSchema()->getFieldNames(), recordBuffer, recordEntry);
}

Record PagedVectorRef::readRecordKeys(const nautilus::val<uint64_t>& pos) const
{
    auto [recordEntry, recordBufferAddr] = getTupleBufferAndPosForEntry(pos);
    auto recordBuffer = RecordBuffer(recordBufferAddr);

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
    return at(getTotalNumberOfEntries());
}

bool PagedVectorRef::operator==(const PagedVectorRef& other) const
{
    if (this == &other)
    {
        return true;
    }

    return memoryLayout == other.memoryLayout && pagedVectorRef == other.pagedVectorRef && numPages == other.numPages
        && firstPage == other.firstPage;
}

PagedVectorRef::TupleBufferAndPosForEntry PagedVectorRef::getTupleBufferAndPosForEntry(nautilus::val<uint64_t> entryPos) const
{
    for (nautilus::val<uint64_t> i = 0; i < numPages; ++i)
    {
        auto currPageAddr = firstPage + i * sizeof(Memory::TupleBuffer);
        auto numTuplesOnPage = RecordBuffer(currPageAddr).getNumRecords();

        if (entryPos < numTuplesOnPage)
        {
            return TupleBufferAndPosForEntry(entryPos, currPageAddr);
        }

        entryPos -= numTuplesOnPage;
    }

    return TupleBufferAndPosForEntry(0, nullptr);
}

nautilus::val<uint64_t> PagedVectorRef::getTotalNumberOfEntries() const
{
    nautilus::val<uint64_t> totalNumEntries = 0;
    for (nautilus::val<uint64_t> i = 0; i < numPages; ++i)
    {
        auto currPage = RecordBuffer(firstPage + i * sizeof(Memory::TupleBuffer));
        totalNumEntries += currPage.getNumRecords();
    }
    return totalNumEntries;
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
