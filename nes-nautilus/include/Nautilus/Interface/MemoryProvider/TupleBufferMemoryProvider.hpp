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
#include <API/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <val_ptr.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Nautilus::Interface::MemoryProvider
{


/// This class takes care of reading and writing data from/to a TupleBuffer.
/// A TupleBufferMemoryProvider is closely coupled with a memory layout and we support row and column layouts, currently.
class TupleBufferMemoryProvider
{
public:
    virtual ~TupleBufferMemoryProvider();

    static std::shared_ptr<TupleBufferMemoryProvider> create(uint64_t bufferSize, const Schema& schema);

    [[nodiscard]] virtual std::shared_ptr<Memory::MemoryLayouts::MemoryLayout> getMemoryLayout() const = 0;

    /// Reads a record from the given bufferAddress and recordIndex.
    /// @param projections: Stores what fields, the Record should contain. If {}, then Record contains all fields available
    /// @param recordBuffer: Stores the memRef to the memory segment of a tuplebuffer, e.g., tuplebuffer.getBuffer()
    /// @param recordIndex: Index of the record to be read
    virtual Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const
        = 0;

    /// Writes a record from the given bufferAddress and recordIndex.
    /// @param recordBuffer: Stores the memRef to the memory segment of a tuplebuffer, e.g., tuplebuffer.getBuffer()
    /// @param recordIndex: Index of the record to be stored to
    /// @param rec: Record to be stored
    /// @param context: ExecutionContext to be used for the write operation
    virtual void writeRecord(
        nautilus::val<uint64_t>& recordIndex,
        const RecordBuffer& recordBuffer,
        const Record& rec,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) const
        = 0;

protected:
    /// Currently, this method does not support Null handling. It loads an VarVal of type from the fieldReference
    /// We require the recordBuffer, as we store variable sized data in a childbuffer and therefore, we need access
    /// to the buffer if the type is of variable sized
    static VarVal loadValue(const PhysicalType& type, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference);

    /// Currently, this method does not support Null handling. It stores an VarVal of type to the fieldReference
    /// We require the recordBuffer, as we store variable sized data in a childbuffer and therefore, we need access
    /// to the buffer if the type is of variable sized
    static VarVal storeValue(
        const PhysicalType& type,
        const RecordBuffer& recordBuffer,
        const nautilus::val<int8_t*>& fieldReference,
        VarVal value,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider);

    [[nodiscard]] static bool
    includesField(const std::vector<Record::RecordFieldIdentifier>& projections, const Record::RecordFieldIdentifier& fieldIndex);
};

}
