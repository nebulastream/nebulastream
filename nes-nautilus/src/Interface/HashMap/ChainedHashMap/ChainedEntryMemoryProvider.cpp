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
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <utility>
#include <vector>

#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

std::pair<std::vector<FieldOffsets>, std::vector<FieldOffsets>> ChainedEntryMemoryProvider::createFieldOffsets(
    const Schema<QualifiedUnboundField, Ordered>& schema,
    const std::vector<Record::RecordFieldIdentifier>& fieldNameKeys,
    const std::vector<Record::RecordFieldIdentifier>& fieldNameValues)
{
    /// For now, we assume that we the fields lie consecutively in the memory like in a row layout.
    /// First, the key fields and then the value fields.
    /// The key and values start after the ChainedHashMapEntry and its hash, see @ref ChainedHashMapEntry
    std::vector<FieldOffsets> fieldsKey;
    std::vector<FieldOffsets> fieldsValue;
    uint64_t offset = sizeof(ChainedHashMapEntry);
    for (const auto& fieldName : fieldNameKeys)
    {
        const auto field = schema[fieldName];
        INVARIANT(field.has_value(), "Field {} not found in schema", fieldName);
        const auto& fieldValue = field.value();
        fieldsKey.emplace_back(
            FieldOffsets{.fieldIdentifier = fieldValue.getFullyQualifiedName(), .type = fieldValue.getDataType(), .fieldOffset = offset});
        offset += fieldValue.getDataType().getSizeInBytesWithNull();
    }

    for (const auto& fieldName : fieldNameValues)
    {
        const auto field = schema[fieldName];
        INVARIANT(field.has_value(), "Field {} not found in schema", fieldName);
        const auto& fieldValue = field.value();
        fieldsValue.emplace_back(
            FieldOffsets{.fieldIdentifier = fieldValue.getFullyQualifiedName(), .type = fieldValue.getDataType(), .fieldOffset = offset});
        offset += fieldValue.getDataType().getSizeInBytesWithNull();
    }
    return {fieldsKey, fieldsValue};
}

namespace
{
/// A ChainedHashMap stores a varsized payload behind a uint32_t length prefix and keeps a raw pointer to it in the entry slot,
/// rather than a VariableSizedAccess into a child buffer.
LoadVarSized makeChainedEntryVarSizedLoadFunction()
{
    return [](const nautilus::val<int8_t*>& fieldSlot) -> std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
    {
        const auto varSizedDataPtr = nautilus::invoke(+[](int8_t** memoryAddressInEntry) { return *memoryAddressInEntry; }, fieldSlot);
        const auto sizeOfVarSized = readValueFromMemRef<uint32_t>(varSizedDataPtr);
        const auto payloadOffset = nautilus::val<uint32_t>(sizeof(uint32_t));
        return {varSizedDataPtr + payloadOffset, sizeOfVarSized};
    };
}

StoreVarSized makeChainedEntryVarSizedStoreFunction(
    const nautilus::val<TupleBuffer*>& hashMapBuffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    return [hashMapBuffer, bufferProvider](
               const nautilus::val<int8_t*>& fieldSlot, const nautilus::val<int8_t*>& data, const nautilus::val<uint64_t>& size)
    {
        nautilus::invoke(
            +[](TupleBuffer* tupleBuffer,
                AbstractBufferProvider* bufferProvider,
                const int8_t** memoryAddressInEntry,
                const int8_t* varSizedData,
                const uint64_t varSizedDataSize)
            {
                constexpr size_t sizeOfIndex = sizeof(uint32_t);
                auto chm = ChainedHashMap::load(*tupleBuffer);
                auto spaceForVarSizedData = chm.allocateSpaceForVarSized(bufferProvider, varSizedDataSize + sizeOfIndex);
                const std::span<const int8_t> varSizedSpan{varSizedData, varSizedData + varSizedDataSize};
                *reinterpret_cast<uint32_t*>(spaceForVarSizedData.data()) = varSizedDataSize;
                std::ranges::copy(std::as_bytes(varSizedSpan), spaceForVarSizedData.begin() + sizeOfIndex);
                *memoryAddressInEntry = reinterpret_cast<const signed char*>(spaceForVarSizedData.data());
            },
            hashMapBuffer,
            bufferProvider,
            fieldSlot,
            data,
            size);
    };
}
}

VarVal ChainedEntryMemoryProvider::readVarVal(
    const nautilus::val<ChainedHashMapEntry*>& entryRef, const Record::RecordFieldIdentifier& fieldName) const
{
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        if (fieldIdentifier == fieldName)
        {
            const auto& entryRefCopy = entryRef;
            const auto castedEntryAddress = static_cast<nautilus::val<int8_t*>>(entryRefCopy);
            return loadFieldValue(type, castedEntryAddress + fieldOffset, makeChainedEntryVarSizedLoadFunction());
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

namespace
{
void writeVarVal(
    const VarVal& value,
    const nautilus::val<int8_t*>& fieldAddress,
    const DataType& type,
    const nautilus::val<TupleBuffer*>& hashMapTupleBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    storeFieldValue(type, fieldAddress, value, makeChainedEntryVarSizedStoreFunction(hashMapTupleBuffer, bufferProvider));
}
}

void ChainedEntryMemoryProvider::writeRecord(
    const nautilus::val<ChainedHashMapEntry*>& entryRef,
    const nautilus::val<TupleBuffer*>& hashMapBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const Record& record) const
{
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        const auto& value = record.read(fieldIdentifier);
        auto castedEntryAddress = static_cast<nautilus::val<int8_t*>>(entryRef);
        writeVarVal(value, castedEntryAddress + fieldOffset, type, hashMapBuffer, bufferProvider);
    }
}

void ChainedEntryMemoryProvider::writeEntryRef(
    const nautilus::val<ChainedHashMapEntry*>& entryRef,
    const nautilus::val<TupleBuffer*>& hashMapBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<ChainedHashMapEntry*>& otherEntryRef) const
{
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fields))
    {
        const auto value = readVarVal(otherEntryRef, fieldIdentifier);
        auto castedEntryAddress = static_cast<nautilus::val<int8_t*>>(entryRef);
        writeVarVal(value, castedEntryAddress + fieldOffset, type, hashMapBuffer, bufferProvider);
    }
}

std::vector<Record::RecordFieldIdentifier> ChainedEntryMemoryProvider::getAllFieldIdentifiers() const
{
    std::vector<Record::RecordFieldIdentifier> fieldIdentifiers;
    for (const auto& [fieldIdentifier, type, fieldOffset] : fields)
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
