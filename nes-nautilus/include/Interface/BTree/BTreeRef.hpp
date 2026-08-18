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
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
class CompilationContext;

class BTreeTupleLayout
{
public:
    virtual ~BTreeTupleLayout() = default;
    [[nodiscard]] virtual uint64_t getSizeInBytes() const = 0;
    [[nodiscard]] virtual Record readRecord(nautilus::val<int8_t*> recordMemAddress, LoadVarSized loadVarSized) const = 0;
    virtual void writeRecord(const Record& record, nautilus::val<int8_t*> memoryForRecord, StoreVarSized storeVarSized) = 0;
};

struct DefaultBTreeTupleLayout final : BTreeTupleLayout
{
private:
    Schema<QualifiedUnboundField, Ordered> schema;

public:
    explicit DefaultBTreeTupleLayout(const Schema<QualifiedUnboundField, Ordered>& schema) : schema(schema) { }

    [[nodiscard]] uint64_t getSizeInBytes() const override { return schema.getSizeInBytes(); }

    [[nodiscard]] Record readRecord(nautilus::val<int8_t*> recordMemAddress, LoadVarSized loadVarSized) const override;
    void writeRecord(const Record& record, nautilus::val<int8_t*> memoryForRecord, StoreVarSized storeVarSized) override;
};

class BTreeComparator
{
public:
    using RecordComparator = std::function<nautilus::val<bool>(const Record&, const Record&)>;

    BTreeComparator(
        CompilationContext& context,
        std::shared_ptr<BTreeTupleLayout> tupleLayout,
        const std::string& comparatorKey,
        RecordComparator comparator);

private:
    friend class BTreeRef;

    [[nodiscard]] bool compare(TupleBuffer* treeBuffer, int8_t* lhs, int8_t* rhs) const;

    std::function<bool(TupleBuffer*, int8_t*, int8_t*)> comparator;
};

/// Record-facing Nautilus interface for BTree. The comparator must define a strict weak ordering.
class BTreeRef
{
public:
    BTreeRef(NautilusBuffer btreeBuffer, std::shared_ptr<BTreeTupleLayout> tupleLayout);

    /// Inserts a record while preserving comparator order. Comparator-equivalent records are retained.
    void
    append(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider, const BTreeComparator& comparator) const;

    /// Returns the record at the zero-based position in comparator order.
    [[nodiscard]] Record at(const nautilus::val<uint64_t>& index) const;

    [[nodiscard]] nautilus::val<uint64_t> size() const;

private:
    static void appendRecord(
        TupleBuffer* buffer,
        AbstractBufferProvider* bufferProvider,
        const BTreeComparator* comparator,
        const int8_t* entry,
        uint64_t entrySize);
    [[nodiscard]] Record read(const nautilus::val<int8_t*>& address) const;

    NautilusBuffer btreeBuffer;
    std::shared_ptr<BTreeTupleLayout> tupleLayout;
};
}
