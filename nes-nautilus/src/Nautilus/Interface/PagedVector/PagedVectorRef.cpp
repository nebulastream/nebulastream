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
PagedVectorRef::PagedVectorRef(const nautilus::val<PagedVector*>& pagedVectorRef, Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout)
    : pagedVectorRef(pagedVectorRef)
    , memoryProvider(MemoryProvider::TupleBufferMemoryProvider::create(memoryLayout->getBufferSize(), memoryLayout->getSchema()))
{
}

void allocateNewPageProxy(PagedVector* pagedVector)
{
    return pagedVector->appendPage();
}

void PagedVectorRef::writeRecord(const Record& record)
{
    (void)record;
}

Record PagedVectorRef::readRecord(const nautilus::val<uint64_t>& pos)
{
    (void)pos;
    Record record;
    return record;
}

Record readRecordKeys(const nautilus::val<uint64_t>& pos)
{
    (void)pos;
    Record record;
    return record;
}

bool PagedVectorRef::operator==(const PagedVectorRef& other) const
{
    if (this == &other)
    {
        return true;
    }

    return memoryLayout == other.memoryLayout && pagedVectorRef == other.pagedVectorRef;
}

}
