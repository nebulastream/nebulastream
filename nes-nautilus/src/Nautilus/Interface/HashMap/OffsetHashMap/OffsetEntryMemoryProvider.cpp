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
#include <Nautilus/Interface/HashMap/OffsetHashMap/OffsetEntryMemoryProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Nautilus/DataStructures/OffsetHashMapWrapper.hpp>
#include <nautilus/val_ptr.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Interface::MemoryProvider {

using AbstractBufferProvider = Memory::AbstractBufferProvider;

std::pair<std::vector<MemoryProvider::OffsetFieldOffsets>, std::vector<MemoryProvider::OffsetFieldOffsets>>
OffsetEntryMemoryProvider::createOffsetFieldOffsets(
    const Schema& schema,
    const std::vector<Record::RecordFieldIdentifier>& fieldNameKeys,
    const std::vector<Record::RecordFieldIdentifier>& fieldNameValues)
{
    std::vector<OffsetFieldOffsets> keyFields;
    std::vector<OffsetFieldOffsets> valueFields;
    
    // OffsetEntry layout: | nextOffset (4) | padding (4) | hash (8) | data[keySize + valueSize] |
    // Data starts at OffsetEntry::headerSize() (16 bytes)
    constexpr uint64_t dataOffset = DataStructures::OffsetEntry::headerSize();
    uint64_t currentOffset = dataOffset;
    
    // Process key fields
    for (const auto& fieldName : fieldNameKeys) {
        const auto field = schema.getFieldByName(fieldName);
        if (field.has_value()) {
            const auto& dataType = field->dataType;
            const auto size = dataType.getSizeInBytes();
            keyFields.emplace_back(OffsetFieldOffsets{fieldName, dataType, currentOffset});
            currentOffset += size;
        }
    }
    
    // Process value fields
    for (const auto& fieldName : fieldNameValues) {
        const auto field = schema.getFieldByName(fieldName);
        if (field.has_value()) {
            const auto& dataType = field->dataType;
            const auto size = dataType.getSizeInBytes();
            valueFields.emplace_back(OffsetFieldOffsets{fieldName, dataType, currentOffset});
            currentOffset += size;
        }
    }
    
    return {keyFields, valueFields};
}

VarVal OffsetEntryMemoryProvider::readVarVal(
    const nautilus::val<DataStructures::OffsetEntry*>& entryRef,
    const Record::RecordFieldIdentifier& fieldName) const
{
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        if (fieldIdentifier == fieldName)
        {
            const auto& entryRefCopy = entryRef;
            auto base = static_cast<nautilus::val<int8_t*>>(entryRefCopy);
            const auto memoryAddress
                = base + nautilus::val<uint64_t>(DataStructures::OffsetEntry::headerSize() + fieldOffset);
            if (type.isType(DataType::Type::VARSIZED_POINTER_REP))
            {
                const auto varSizedDataPtr
                    = nautilus::invoke(+[](const int8_t** memAddrInEntry) { return *memAddrInEntry; }, memoryAddress);
                VariableSizedData varSizedData(varSizedDataPtr);
                return varSizedData;
            }

            const auto varVal = VarVal::readVarValFromMemory(memoryAddress, type.type);
            return varVal;
        }
    }
    throw FieldNotFound("Field {} not found in OffsetEntryMemoryProvider", fieldName);
}

Record OffsetEntryMemoryProvider::readRecord(const nautilus::val<DataStructures::OffsetEntry*>& entryRef) const
{
    Record record;
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        const auto value = readVarVal(entryRef, fieldIdentifier);
        record.write(fieldIdentifier, value);
    }
    return record;
}

namespace {
void storeVarSized(
    const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
    const nautilus::val<AbstractBufferProvider*>& bufferProviderRef,
    const nautilus::val<int8_t*>& memoryAddress,
    const VariableSizedData& variableSizedData)
{
    nautilus::invoke(
        +[](DataStructures::OffsetHashMapWrapper* map,
            AbstractBufferProvider* bufferProvider,
            const int8_t** memoryAddressInEntry,
            const int8_t* varSizedData,
            const uint64_t varSizedDataSize)
        {
            auto* const space = map->allocateSpaceForVarSized(bufferProvider, varSizedDataSize);
            std::memcpy(space, varSizedData, varSizedDataSize);
            *memoryAddressInEntry = space;
        },
        hashMapRef,
        bufferProviderRef,
        memoryAddress,
        variableSizedData.getReference(),
        variableSizedData.getTotalSize());
}
}

void OffsetEntryMemoryProvider::writeRecord(
    const nautilus::val<DataStructures::OffsetEntry*>& entryRef,
    const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const Record& record) const
{
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        const auto& value = record.read(fieldIdentifier);
        const auto& entryRefCopy = entryRef;
        auto base = static_cast<nautilus::val<int8_t*>>(entryRefCopy);
        const auto memoryAddress
            = base + nautilus::val<uint64_t>(DataStructures::OffsetEntry::headerSize() + fieldOffset);
        if (type.isType(DataType::Type::VARSIZED_POINTER_REP))
        {
            auto varSizedValue = value.cast<VariableSizedData>();
            storeVarSized(hashMapRef, bufferProvider, memoryAddress, varSizedValue);
        }
        else
        {
            value.writeToMemory(memoryAddress);
        }
    }
}

void OffsetEntryMemoryProvider::writeEntryRef(
    const nautilus::val<DataStructures::OffsetEntry*>& entryRef,
    const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<DataStructures::OffsetEntry*>& otherEntryRef) const
{
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        const auto value = readVarVal(otherEntryRef, fieldIdentifier);
        const auto memoryAddress = static_cast<nautilus::val<int8_t*>>(entryRef)
            + nautilus::val<uint64_t>(DataStructures::OffsetEntry::headerSize() + fieldOffset);
        if (type.isType(DataType::Type::VARSIZED_POINTER_REP))
        {
            auto varSizedValue = value.cast<VariableSizedData>();
            storeVarSized(hashMapRef, bufferProvider, memoryAddress, varSizedValue);
        }
        else
        {
            value.writeToMemory(memoryAddress);
        }
    }
}

std::vector<Record::RecordFieldIdentifier> OffsetEntryMemoryProvider::getAllFieldIdentifiers() const
{
    std::vector<Record::RecordFieldIdentifier> ids;
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        ids.push_back(fieldIdentifier);
    }
    return ids;
}

const std::vector<OffsetFieldOffsets>& OffsetEntryMemoryProvider::getAllFields() const { return fields; }

}
