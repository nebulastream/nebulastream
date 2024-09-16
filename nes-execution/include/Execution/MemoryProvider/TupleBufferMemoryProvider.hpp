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

#include <Nautilus/Interface/Record.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

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

    static MemoryProviderPtr createMemoryProvider(const uint64_t bufferSize, const SchemaPtr schema);

    virtual MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() = 0;

    /// Reads a record from the given bufferAddress and recordIndex.
    /// @param projections: Stores what fields, the Record should contain. If {}, then Record contains all fields available
    /// @param bufferAddress: Stores the memRef to the memory segment of a tuplebuffer, e.g., tuplebuffer.getBuffer()
    /// @param recordIndex: Index of the record to be read
    virtual Nautilus::Record readRecord(
        const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
        nautilus::val<int8_t*>& bufferAddress,
        nautilus::val<uint64_t>& recordIndex) const
        = 0;

    /// Writes a record from the given bufferAddress and recordIndex.
    /// @param bufferAddress: Stores the memRef to the memory segment of a tuplebuffer, e.g., tuplebuffer.getBuffer()
    /// @param recordIndex: Index of the record to be stored to
    virtual void writeRecord(nautilus::val<uint64_t>& recordIndex, nautilus::val<int8_t*>& bufferAddress, NES::Nautilus::Record& rec) const
        = 0;

    /// Currently, this method does not support Null handling. It loads an VarVal of type from the fieldReference
    /// We require the bufferReference, as we store variable sized data in a childbuffer and therefore, we need access
    /// to the buffer if the type is of variable sized
    static Nautilus::VarVal
    load(const PhysicalTypePtr& type, const nautilus::val<int8_t*>& bufferReference, nautilus::val<int8_t*>& fieldReference);

    /// Currently, this method does not support Null handling. It stores an VarVal of type to the fieldReference
    /// We require the bufferReference, as we store variable sized data in a childbuffer and therefore, we need access
    /// to the buffer if the type is of variable sized
    static Nautilus::VarVal store(
        const NES::PhysicalTypePtr& type,
        const nautilus::val<int8_t*>& bufferReference,
        nautilus::val<int8_t*>& fieldReference,
        Nautilus::VarVal value);

    [[nodiscard]] static bool includesField(
        const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections, const Nautilus::Record::RecordFieldIdentifier& fieldIndex);
};

} /// namespace NES::Runtime::Execution::MemoryProvider
