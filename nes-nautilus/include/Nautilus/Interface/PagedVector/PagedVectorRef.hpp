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

#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::Interface
{
/// Forward declaration of PagedVectorRefIter so that we can use it in PagedVectorRef
class PagedVectorRefIter;

/// This class is a nautilus interface to our PagedVector. It provides a way to write and read records to and from the PagedVector
/// Writing and reading records from a PagedVector should be ONLY done via this class. This class is not thread-safe.
class PagedVectorRef
{
public:
    /// Declaring PagedVectorRefIter a friend class such that we can access the private members
    friend class PagedVectorRefIter;
    PagedVectorRef(
        const nautilus::val<PagedVector*>& pagedVectorRef,
        const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider);

    /// Writes a new record to the pagedVectorRef
    /// @param record the new record to be written
    /// @param bufferProvider: Buffer provider used for acquiring memory for the write operation, if needed
    void writeRecord(const Record& record, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) const;

    /// @brief Reads the specified fields of a record from the pagedVectorRef
    /// @param pos record position in pagedVector
    /// @param projections the desired fields of the record
    [[nodiscard]] Record
    readRecord(const nautilus::val<uint64_t>& pos, const std::vector<Record::RecordFieldIdentifier>& projections) const;

    [[nodiscard]] PagedVectorRefIter begin(const std::vector<Record::RecordFieldIdentifier>& projections) const;
    [[nodiscard]] PagedVectorRefIter end(const std::vector<Record::RecordFieldIdentifier>& projections) const;
    nautilus::val<bool> operator==(const PagedVectorRef& other) const;
    nautilus::val<uint64_t> getNumberOfTuples() const;

private:
    nautilus::val<PagedVector*> pagedVectorRef;
    std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
    nautilus::val<Memory::MemoryLayouts::MemoryLayout*> memoryLayout;
};

class PagedVectorRefIter
{
public:
    explicit PagedVectorRefIter(
        PagedVectorRef pagedVector,
        const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<Memory::TupleBuffer*>& curPage,
        const nautilus::val<uint64_t>& posOnPage,
        const nautilus::val<uint64_t>& pos,
        const nautilus::val<uint64_t>& numberOfTuplesInPagedVector);

    Record operator*() const;
    PagedVectorRefIter& operator++();
    nautilus::val<bool> operator==(const PagedVectorRefIter& other) const;
    nautilus::val<bool> operator!=(const PagedVectorRefIter& other) const;
    nautilus::val<uint64_t> operator-(const PagedVectorRefIter& other) const;

private:
    PagedVectorRef pagedVector;
    std::vector<Record::RecordFieldIdentifier> projections;
    nautilus::val<uint64_t> pos;
    nautilus::val<uint64_t> numberOfTuplesInPagedVector;
    nautilus::val<uint64_t> posOnPage;
    nautilus::val<Memory::TupleBuffer*> curPage;
    std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
};

}
