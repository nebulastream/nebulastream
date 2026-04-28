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
#include <ranges>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// @brief Forward declaration of PagedVectorRefIter so that we can use it in PagedVectorRef
class PagedVectorRefIter;


using LoadFunction = std::function<std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>(nautilus::val<int8_t*> fieldSlot)>;
using AllocateFunction = std::function<nautilus::val<int8_t*>(nautilus::val<int8_t*> fieldSlot, nautilus::val<uint64_t> allocationSize)>;

///  @brief The schema of a tuple for the paged vector. It's the same as the Schema specified in the constructor, but pre-computes and stores the field offsets.
class TupleSchema
{
public:
    TupleSchema() = delete;
    explicit TupleSchema(const Schema& schema);

    struct Field
    {
        Record::RecordFieldIdentifier name;
        DataType dataType;
        uint64_t fieldOffset;
    };

    std::vector<Field> fields;

    uint64_t getNumberOfFields() const { return fields.size(); };
};

/// @brief This class is the interface for creating different tuple layouts
class TupleLayout
{
public:
    virtual ~TupleLayout() = default;
    virtual const std::vector<TupleSchema::Field>& getFields() const = 0;
    virtual TupleSchema::Field getFieldAt(uint64_t pos) const = 0;
    virtual const std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const = 0;
    virtual size_t getNumberOfFields() const = 0;
    virtual size_t getTupleSize() const = 0;

    /// @brief Reads a record from the specified memory address. The address is expected to point at the beginning of the record to be read.
    /// The LoadFunction handles the varsized data loading, as it is stored in a separate buffer.
    virtual Record readRecord(const nautilus::val<std::int8_t*> recordMemAddress, LoadFunction) const = 0;

    /// @brief Writes a record to the specified memory address (which should be its correct memory offset inside the correct page, which enough space allocated).
    /// The allocateVarSized method takes care of allocating extra space for varsized if necessary.
    virtual void writeRecord(const Record& record, nautilus::val<std::int8_t*> memoryForRecord, AllocateFunction allocateVarSized) = 0;
};

/// @brief This class is a basic row-oriented layout of tuples for the paged vector
struct BasicTupleLayout final : TupleLayout
{
private:
    TupleSchema schema;

public:
    explicit BasicTupleLayout(const TupleSchema& schema) : schema(schema) { }

    const std::vector<TupleSchema::Field>& getFields() const override { return schema.fields; }

    TupleSchema::Field getFieldAt(uint64_t pos) const override;

    const std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const override;

    size_t getTupleSize() const override;

    size_t getNumberOfFields() const override;

    Record readRecord(const nautilus::val<std::int8_t*> recordMemAddress, LoadFunction loadFunc) const override;

    void writeRecord(const Record& record, nautilus::val<std::int8_t*> memoryForRecord, AllocateFunction allocateVarSized) override;
};

/// @brief This class is a nautilus interface to our PagedVector. It provides a way to write and read records to and from the PagedVector
/// Writing and reading records from a PagedVector should be ONLY done via this class. This class is not thread-safe.
class PagedVectorRef
{
public:
    /// @brief Declaring PagedVectorRefIter a friend class such that we can access the private members
    friend class PagedVectorRefIter;
    PagedVectorRef(const nautilus::val<TupleBuffer*>& pagedVectorBuffer, std::shared_ptr<TupleLayout> tupleLayout);

    /// Appends the record to the paged vector, extending its total size and possibly allocating a new page.
    void push_back(const Record& record, nautilus::val<AbstractBufferProvider*> bufferProvider);

    /// @brief Access method for getting a specific record from the paged vector.
    /// The entryPos refers to the record's global position in the paged vector and is not page-specific.
    [[nodiscard]] Record at(const nautilus::val<uint64_t>& entryPos) const;

    [[nodiscard]] PagedVectorRefIter begin() const;
    [[nodiscard]] PagedVectorRefIter end() const;
    nautilus::val<bool> operator==(const PagedVectorRef& other) const;

    /// @brief Returns the total number of records in the Paged Vector
    [[nodiscard]] nautilus::val<uint64_t> getNumberOfRecords() const;

private:
    /// @brief Holds a reference to the main paged vector buffer
    nautilus::val<TupleBuffer*> pagedVectorBuffer;
    /// @brief specifies how tuples are stored in the paged vector's pages
    std::shared_ptr<TupleLayout> tupleLayout;
};

class PagedVectorRefIter
{
public:
    explicit PagedVectorRefIter(
        PagedVectorRef pagedVector,
        const std::shared_ptr<TupleLayout>& tupleLayout,
        const nautilus::val<TupleBuffer*>& curPage,
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
    nautilus::val<TupleBuffer*> curPage;
    std::shared_ptr<TupleLayout> tupleLayout;
};

}
