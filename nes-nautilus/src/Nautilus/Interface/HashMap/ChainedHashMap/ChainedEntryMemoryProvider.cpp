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

#include <cstdint>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val.hpp>


namespace NES::Nautilus::Interface::MemoryProvider
{

std::pair<std::vector<MemoryProvider::FieldOffsets>, std::vector<MemoryProvider::FieldOffsets>>
ChainedEntryMemoryProvider::createFieldOffsets(
    const Schema& schema,
    const std::vector<Record::RecordFieldIdentifier>& fieldNameKeys,
    const std::vector<Record::RecordFieldIdentifier>& fieldNameValues)
{
    /// For now, we assume that we the fields lie consecutively in the memory like in a row layout.
    /// First, the key fields and then the value fields.
    /// The key and values start after the ChainedHashMapEntry and its hash, see @ref ChainedHashMapEntry
    std::vector<MemoryProvider::FieldOffsets> fieldsKey;
    std::vector<MemoryProvider::FieldOffsets> fieldsValue;
    uint64_t offset = sizeof(ChainedHashMapEntry);
    for (const auto& fieldName : fieldNameKeys)
    {
        const auto field = schema.getFieldByName(fieldName);
        INVARIANT(field.has_value(), "Field {} not found in schema", fieldName);
        const auto& fieldValue = field.value();
        fieldsKey.emplace_back(MemoryProvider::FieldOffsets{fieldValue.name, fieldValue.dataType, offset});
        offset += fieldValue.dataType.getSizeInBytes();
    }

    for (const auto& fieldName : fieldNameValues)
    {
        const auto field = schema.getFieldByName(fieldName);
        INVARIANT(field.has_value(), "Field {} not found in schema", fieldName);
        const auto& fieldValue = field.value();
        fieldsValue.emplace_back(MemoryProvider::FieldOffsets{fieldValue.name, fieldValue.dataType, offset});
        offset += fieldValue.dataType.getSizeInBytes();
    }
    return {fieldsKey, fieldsValue};
}

VarVal ChainedEntryMemoryProvider::readVarVal(
    const nautilus::val<ChainedHashMapEntry*>& entryRef, const Record::RecordFieldIdentifier& fieldName) const
{
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        if (fieldIdentifier == fieldName)
        {
            const auto& entryRefCopy = entryRef;
            auto castedEntryAddress = static_cast<nautilus::val<int8_t*>>(entryRefCopy);
            const auto memoryAddress = castedEntryAddress + fieldOffset;
            const auto varVal = VarVal::readVarValFromMemory(memoryAddress, type.type);
            return varVal;
        }
    }
    throw FieldNotFound("Field {} not found in ChainedEntryMemoryProvider", fieldName);
}

Record ChainedEntryMemoryProvider::readRecord(const nautilus::val<ChainedHashMapEntry*>& entryRef) const
{
    Record record;
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        const auto value = readVarVal(entryRef, fieldIdentifier);
        record.write(fieldIdentifier, value);
    }

    return record;
}

void ChainedEntryMemoryProvider::writeRecord(const nautilus::val<ChainedHashMapEntry*>& entryRef, const Record& record) const
{
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        const auto& value = record.read(fieldIdentifier);
        const auto& entryRefCopy = entryRef;
        auto castedEntryAddress = static_cast<nautilus::val<int8_t*>>(entryRefCopy);
        const auto memoryAddress = castedEntryAddress + fieldOffset;
        value.writeToMemory(memoryAddress);
    }
}

void ChainedEntryMemoryProvider::writeEntryRef(
    const nautilus::val<ChainedHashMapEntry*>& entryRef, const nautilus::val<ChainedHashMapEntry*>& otherEntryRef) const
{
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        const auto value = readVarVal(otherEntryRef, fieldIdentifier);
        const auto memoryAddress = static_cast<nautilus::val<int8_t*>>(entryRef) + nautilus::val<uint64_t>(fieldOffset);
        value.writeToMemory(memoryAddress);
    }
}

std::vector<Record::RecordFieldIdentifier> ChainedEntryMemoryProvider::getAllFieldIdentifiers() const
{
    std::vector<Record::RecordFieldIdentifier> fieldIdentifiers;
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        fieldIdentifiers.push_back(fieldIdentifier);
    }
    return fieldIdentifiers;
}

const std::vector<FieldOffsets>& ChainedEntryMemoryProvider::getAllFields() const
{
    return fields;
}

}
