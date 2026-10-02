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
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// Field offsets of one schema in row layout, computed once and reused for every record.
///
/// Everything else in the engine reaches a TupleBuffer through Nautilus, i.e. from
/// generated code. An asynchronously executed operator runs on a plain thread outside
/// that code, so it needs this: ordinary C++ access to the same bytes.
///
/// Row layout is a prefix sum of `getSizeInBytesWithNull()` over the ordered schema —
/// the same arithmetic `LowerSchemaProvider` performs for the Nautilus side.
///
/// Nullable fields are rejected. They would need the engine's null encoding, and no
/// current user declares them: `CREATE SEMANTIC MODEL` binds its fields as NOT NULL.
class AsyncRecordLayout
{
public:
    explicit AsyncRecordLayout(const Schema<UnqualifiedUnboundField, Ordered>& schema);

    [[nodiscard]] size_t tupleSize() const { return tupleSizeInBytes; }
    [[nodiscard]] size_t fieldCount() const { return fields.size(); }

    /// How many records of this layout fit into a buffer of `bufferSize` bytes.
    [[nodiscard]] uint64_t capacity(uint64_t bufferSize) const;

    /// Index of a field by its canonical name, or nullopt if the schema has no such field.
    [[nodiscard]] std::optional<size_t> indexOf(std::string_view fieldName) const;

    /// Like `indexOf`, but reads `columnName` as an SQL identifier first: unquoted names are
    /// upper-cased, so `reviewText` finds the field the schema stores as `REVIEWTEXT`. Use
    /// this for names that came from a query or a configuration value.
    [[nodiscard]] std::optional<size_t> indexOfColumn(std::string_view columnName) const;

    [[nodiscard]] const std::string& nameOf(size_t fieldIndex) const;
    [[nodiscard]] const DataType& typeOf(size_t fieldIndex) const;
    [[nodiscard]] size_t offsetOf(size_t fieldIndex) const;

private:
    struct Field
    {
        std::string name;
        DataType type;
        size_t offset;
    };

    std::vector<Field> fields;
    size_t tupleSizeInBytes = 0;
};

/// Read access to a single record inside a TupleBuffer.
class AsyncRecordView
{
public:
    AsyncRecordView(const AsyncRecordLayout& layout, const TupleBuffer& buffer, uint64_t recordIndex);

    /// Contents of a VARSIZED field. The view points into the buffer's child buffer and
    /// stays valid as long as the buffer does.
    [[nodiscard]] std::string_view readText(size_t fieldIndex) const;

    /// Any field rendered as text — VARSIZED verbatim, everything else formatted the way
    /// the Python reference's `str()` renders it. This is what prompt building needs.
    [[nodiscard]] std::string readAsText(size_t fieldIndex) const;

    /// Raw bytes of a fixed-size field.
    [[nodiscard]] std::span<const std::byte> readRaw(size_t fieldIndex) const;

    [[nodiscard]] const AsyncRecordLayout& getLayout() const { return *layout; }

private:
    const AsyncRecordLayout* layout;
    const TupleBuffer* buffer;
    uint64_t recordIndex;
};

/// Write access to a single record in an output buffer.
class AsyncRecordWriter
{
public:
    AsyncRecordWriter(const AsyncRecordLayout& layout, TupleBuffer& buffer, AbstractBufferProvider& bufferProvider, uint64_t recordIndex);

    void writeText(size_t fieldIndex, std::string_view value);
    void writeRaw(size_t fieldIndex, std::span<const std::byte> value);

    /// Counterpart to `AsyncRecordView::readAsText`: takes the value as text and stores it
    /// in the field's declared type. VARSIZED is stored verbatim, everything else is parsed.
    /// Throws when the text does not parse as the declared type.
    void writeAsText(size_t fieldIndex, std::string_view value);

    /// Copies every field of `source` into the field of the same name, if this layout has
    /// one. VARSIZED contents are copied into this buffer's own child buffers, so the
    /// result does not reference the source buffer.
    void copyMatchingFields(const AsyncRecordView& source);

private:
    const AsyncRecordLayout* layout;
    TupleBuffer* buffer;
    AbstractBufferProvider* bufferProvider;
    uint64_t recordIndex;
};

}
