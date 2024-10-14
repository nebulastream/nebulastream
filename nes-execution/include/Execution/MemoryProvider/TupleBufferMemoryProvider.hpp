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

#include <Execution/RecordBuffer.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/Record.hpp>


namespace NES::Runtime::Execution::MemoryProvider
{

class TupleBufferMemoryProvider;
using MemoryProviderPtr = std::unique_ptr<TupleBufferMemoryProvider>;

/// This class takes care of reading and writing data from/to a TupleBuffer.
/// A TupleBufferMemoryProvider is closely coupled with a memory layout and we support row and column layouts, currently.
class TupleBufferMemoryProvider
{
public:
    virtual ~TupleBufferMemoryProvider();

    static MemoryProviderPtr create(const uint64_t bufferSize, const SchemaPtr schema);

    virtual Memory::MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() = 0;

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
    virtual void writeRecord(nautilus::val<uint64_t>& recordIndex, const RecordBuffer& recordBuffer, const Record& rec) const = 0;

    /// Currently, this method does not support Null handling. It loads an VarVal of type from the fieldReference
    /// We require the recordBuffer, as we store variable sized data in a childbuffer and therefore, we need access
    /// to the buffer if the type is of variable sized
    static VarVal loadValue(const PhysicalTypePtr& type, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference);

    /// Currently, this method does not support Null handling. It stores an VarVal of type to the fieldReference
    /// We require the recordBuffer, as we store variable sized data in a childbuffer and therefore, we need access
    /// to the buffer if the type is of variable sized
    static VarVal
    storeValue(const PhysicalTypePtr& type, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference, VarVal value);

    [[nodiscard]] static bool
    includesField(const std::vector<Record::RecordFieldIdentifier>& projections, const Record::RecordFieldIdentifier& fieldIndex);
};

}
