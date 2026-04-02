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
#include <span>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// @brief BufferLayout is the base class for all buffer layouts.
/// It defines the general structure of a buffer: an optional header region followed by a data region.
/// Subclasses implement the full strategy for addressing and serializing records (row or column layout).
/// The VarSized and field-value utilities are shared across all layouts as protected static helpers.
class BufferLayoutRef
{
public:
    /// @brief Returns the byte address of the start of the header region within the buffer.
    /// For layouts with no header, this is the same as dataStart.
    [[nodiscard]] virtual nautilus::val<int8_t*> getHeaderStart(const nautilus::val<int8_t*>& bufferBase) const = 0;

    /// @brief Returns the byte address of the start of the data region within the buffer (i.e., after the header).
    [[nodiscard]] virtual nautilus::val<int8_t*> getDataStart(const nautilus::val<int8_t*>& bufferBase) const = 0;

    /// @brief Returns the size of the header in bytes.
    [[nodiscard]] virtual uint64_t getHeaderSize() const = 0;

    /// @brief Returns how many records fit in one buffer.
    [[nodiscard]] virtual uint64_t getCapacity() const = 0;

    /// @brief Returns the total buffer size in bytes.
    [[nodiscard]] virtual uint64_t getBufferSize() const = 0;

    /// @brief Returns all field names known to this layout.
    [[nodiscard]] virtual std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const = 0;

    /// @brief Returns all data types known to this layout.
    [[nodiscard]] virtual std::vector<DataType> getAllDataTypes() const = 0;

    /// Returned by writeRecord to indicate success and number of records written.
    struct WriteRecordResult
    {
        nautilus::val<bool> successful;
        nautilus::val<uint64_t> writtenRecords;
    };

    /// @brief Reads a record at the given recordIndex from the buffer.
    /// @param projections Fields to include in the returned record. If empty, all fields are returned.
    /// @param recordBuffer The buffer to read from.
    /// @param recordIndex The logical index of the record within this buffer.
    [[nodiscard]] virtual Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const = 0;

    /// @brief Writes a record at the given recordIndex into the buffer.
    /// @param recordIndex The logical index of the record within this buffer.
    /// @param recordBuffer The buffer to write into.
    /// @param rec The record to write.
    /// @param bufferProvider Used for acquiring child buffers for variable-sized data.
    virtual WriteRecordResult writeRecord(
        nautilus::val<uint64_t>& recordIndex,
        const RecordBuffer& recordBuffer,
        const Record& rec,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const = 0;

    virtual ~BufferLayoutRef() = default;

    /// @brief Loads variable-sized data from a child buffer attached to tupleBuffer.
    static std::span<std::byte>
    loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, VariableSizedAccess variableSizedAccess) noexcept;

    /// @brief Writes variable-sized data into a child buffer attached to tupleBuffer.
    static VariableSizedAccess
    writeVarSized(TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, std::span<const std::byte> varSizedValue);

protected:
    /// @brief Loads a typed VarVal from the given field memory address.
    static VarVal loadValue(const DataType& type, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference);

    /// @brief Stores a typed VarVal to the given field memory address.
    static VarVal storeValue(
        const DataType& type,
        const RecordBuffer& recordBuffer,
        const nautilus::val<int8_t*>& fieldReference,
        VarVal value,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider);

    /// @brief Returns true if fieldIndex is present in projections.
    [[nodiscard]] static bool
    includesField(const std::vector<Record::RecordFieldIdentifier>& projections, const Record::RecordFieldIdentifier& fieldIndex);
};

} // namespace NES