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
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_concepts.hpp>

namespace NES::Nautilus::Interface::MemoryProvider
{

/// ChainedEntryMemoryProvider uses it to store the offsets for the keys and values of the ChainedHashMapEntry
struct FieldOffsets
{
    Record::RecordFieldIdentifier fieldIdentifier;
    DataType type;
    uint64_t fieldOffset;
};

/// ChainedHashMapEntry uses for reading and writing either the keys or values
class ChainedEntryMemoryProvider
{
public:
    explicit ChainedEntryMemoryProvider(std::vector<FieldOffsets> fields) : fields(std::move(fields)) { }

    /// We need to create the fields for the keys and values here, as we know here how the fields and the values are stored in the ChainedHashMapEntry.
    /// We can use here "normal" C++ values, as only the C++ runtime MUST call this method
    static std::pair<std::vector<MemoryProvider::FieldOffsets>, std::vector<MemoryProvider::FieldOffsets>> createFieldOffsets(
        const Schema& schema,
        const std::vector<Record::RecordFieldIdentifier>& fieldNameKeys,
        const std::vector<Record::RecordFieldIdentifier>& fieldNameValues);


    [[nodiscard]] VarVal
    readVarVal(const nautilus::val<ChainedHashMapEntry*>& entryRef, const Record::RecordFieldIdentifier& fieldName) const;
    [[nodiscard]] Record readRecord(const nautilus::val<ChainedHashMapEntry*>& entryRef) const;
    void writeRecord(
        const nautilus::val<ChainedHashMapEntry*>& entryRef,
        const nautilus::val<ChainedHashMap*>& hashMapRef,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider,
        const Record& record) const;
    void writeEntryRef(
        const nautilus::val<ChainedHashMapEntry*>& entryRef,
        const nautilus::val<ChainedHashMap*>& hashMapRef,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<ChainedHashMapEntry*>& otherEntryRef) const;

    [[nodiscard]] std::vector<Record::RecordFieldIdentifier> getAllFieldIdentifiers() const;
    [[nodiscard]] const std::vector<FieldOffsets>& getAllFields() const;

private:
    std::vector<FieldOffsets> fields;
};

}
