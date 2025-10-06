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
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>
#include <Nautilus/DataStructures/OffsetHashMapWrapper.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES::Nautilus::Interface::MemoryProvider
{

using AbstractBufferProvider = Memory::AbstractBufferProvider;

/// OffsetEntryMemoryProvider uses OffsetFieldOffsets to store the offsets for the keys and values of OffsetEntry
struct OffsetFieldOffsets
{
    Record::RecordFieldIdentifier fieldIdentifier;
    DataType type;
    uint64_t fieldOffset;
};

/// OffsetEntryMemoryProvider handles reading and writing keys or values for OffsetEntry
/// Unlike ChainedEntryMemoryProvider, this works directly with OffsetEntry* and OffsetHashMapWrapper*
class OffsetEntryMemoryProvider
{
public:
    explicit OffsetEntryMemoryProvider(std::vector<OffsetFieldOffsets> fields) : fields(std::move(fields)) { }

    /// Creates field offsets for keys and values based on OffsetEntry memory layout:
    /// | nextOffset (4) | hash (8) | data[keySize + valueSize] |
    static std::pair<std::vector<MemoryProvider::OffsetFieldOffsets>, std::vector<MemoryProvider::OffsetFieldOffsets>> createOffsetFieldOffsets(
        const Schema& schema,
        const std::vector<Record::RecordFieldIdentifier>& fieldNameKeys,
        const std::vector<Record::RecordFieldIdentifier>& fieldNameValues);

    [[nodiscard]] VarVal
    readVarVal(const nautilus::val<DataStructures::OffsetEntry*>& entryRef, const Record::RecordFieldIdentifier& fieldName) const;
    
    [[nodiscard]] Record readRecord(const nautilus::val<DataStructures::OffsetEntry*>& entryRef) const;
    
    void writeRecord(
        const nautilus::val<DataStructures::OffsetEntry*>& entryRef,
        const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const Record& record) const;
        
    void writeEntryRef(
        const nautilus::val<DataStructures::OffsetEntry*>& entryRef,
        const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<DataStructures::OffsetEntry*>& otherEntryRef) const;

    [[nodiscard]] std::vector<Record::RecordFieldIdentifier> getAllFieldIdentifiers() const;
    [[nodiscard]] const std::vector<OffsetFieldOffsets>& getAllFields() const;

private:
    std::vector<OffsetFieldOffsets> fields;
};

}
