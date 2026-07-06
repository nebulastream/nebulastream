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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <functional>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// @brief Forward declaration of PagedVectorRefIter so that we can use it in PagedVectorRef
class PagedVectorRefIter;
class PagedVectorRefIterSentinel;
static constexpr size_t DEFAULT_PAGE_SIZE = 4096U;

/// @brief Load function that reads a varsized field from a specific address in memory
using LoadVarSizedFunction = std::function<std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>(nautilus::val<int8_t*> fieldSlot)>;
/// @brief Allocation function that takes care of allocating the necessary space for the varsized data of a field
using AllocateVarSizedFunction
    = std::function<nautilus::val<int8_t*>(nautilus::val<int8_t*> fieldSlot, nautilus::val<uint64_t> allocationSize)>;

/// @brief This class is the interface for creating different tuple layouts
class PagedVectorTupleLayout
{
public:
    virtual ~PagedVectorTupleLayout() = default;
    [[nodiscard]] virtual const Schema<QualifiedUnboundField, Ordered>& getSchema() const = 0;
    /// @brief Reads a record from the specified memory address. The address is expected to point at the beginning of the record to be read.
    /// The LoadFunction handles the varsized data loading, as it is stored in a separate buffer.
    [[nodiscard]] virtual Record readRecord(nautilus::val<std::int8_t*> recordMemAddress, LoadVarSizedFunction) const = 0;

    /// @brief Writes a record to the specified memory address (which should be its correct memory offset inside the correct page, which enough space allocated).
    /// The allocateVarSized method takes care of allocating extra space for varsized if necessary.
    virtual void writeRecord(const Record& record, nautilus::val<std::int8_t*> memoryForRecord, AllocateVarSizedFunction allocateVarSized)
        = 0;
};

/// @brief This class is the default layout of tuples for the paged vector
struct DefaultPagedVectorTupleLayout final : PagedVectorTupleLayout
{
private:
    Schema<QualifiedUnboundField, Ordered> schema;

public:
    explicit DefaultPagedVectorTupleLayout(const Schema<QualifiedUnboundField, Ordered>& schema) : schema(schema) { }

    [[nodiscard]] const Schema<QualifiedUnboundField, Ordered>& getSchema() const override { return schema; }

    [[nodiscard]] Record readRecord(nautilus::val<std::int8_t*> recordMemAddress, LoadVarSizedFunction loadFunc) const override;

    void writeRecord(const Record& record, nautilus::val<std::int8_t*> memoryForRecord, AllocateVarSizedFunction allocateVarSized) override;
};

/// @brief This class is a nautilus interface to our PagedVector. It provides a way to write and read records to and from the PagedVector
/// Writing and reading records from a PagedVector should be ONLY done via this class. This class is not thread-safe.
class PagedVectorRef
{
public:
    /// @brief Declaring PagedVectorRefIter a friend class such that we can access the private members
    friend class PagedVectorRefIter;
    PagedVectorRef(NautilusBuffer pagedVectorBuffer, std::shared_ptr<PagedVectorTupleLayout> tupleLayout);

    /// Appends the record to the paged vector, extending its total size and possibly allocating a new page.
    void pushBack(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider);

    /// @brief Access method for getting a specific record from the paged vector.
    /// The entryPos refers to the record's global position in the paged vector and is not page-specific.
    [[nodiscard]] Record at(const nautilus::val<uint64_t>& entryPos) const;

    [[nodiscard]] PagedVectorRefIter begin() const;
    [[nodiscard]] PagedVectorRefIterSentinel end() const;

    /// @brief Returns the total number of records in the Paged Vector
    [[nodiscard]] nautilus::val<uint64_t> getNumberOfRecords() const;

private:
    /// @brief Holds a reference to the main paged vector buffer
    NautilusBuffer pagedVectorBuffer;
    /// @brief specifies how tuples are stored in the paged vector's pages
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
};

class PagedVectorRefIter
{
public:
    explicit PagedVectorRefIter(
        PagedVectorRef pagedVector,
        const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout,
        OwnedNautilusBuffer curPage,
        const nautilus::val<uint64_t>& posOnPage,
        const nautilus::val<uint64_t>& pos);

    Record operator*() const;
    PagedVectorRefIter& operator++();
    PagedVectorRefIter operator++(int);
    nautilus::val<bool> operator==(const PagedVectorRefIterSentinel& other) const;
    nautilus::val<bool> operator!=(const PagedVectorRefIterSentinel& other) const;
    nautilus::val<uint64_t> operator-(const PagedVectorRefIter& other) const;

private:
    PagedVectorRef pagedVector;
    mutable nautilus::val<uint64_t> pos;
    mutable nautilus::val<uint64_t> posOnPage;
    /// Lazily-materialized cache of the page that currently holds the element at `pos`. It is mutable because `operator*() const`
    /// reloads it when iteration crosses a page boundary; this is logically const, as it does not change which element is referenced.
    mutable OwnedNautilusBuffer curPage;
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
};

class PagedVectorRefIterSentinel
{
public:
    explicit PagedVectorRefIterSentinel(const nautilus::val<uint64_t>& numTuplesInPagedVector);

    nautilus::val<bool> operator==(const PagedVectorRefIter& other) const;
    nautilus::val<bool> operator!=(const PagedVectorRefIter& other) const;

private:
    friend PagedVectorRefIter;
    nautilus::val<uint64_t> numberOfTuplesInPagedVector;
};

}
