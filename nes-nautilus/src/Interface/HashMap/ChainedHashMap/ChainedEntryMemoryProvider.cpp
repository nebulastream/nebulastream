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
#include <DataTypes/UnboundField.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordLayoutUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_memcpy.hpp>

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
/// Variable-sized payload (VARSIZED, VECTOR, and var-sized members nested in STRUCT / FIXEDSIZED) is deep-copied into the
/// hash map's own var-sized pages, so an entry never references memory of the input buffer it was created from.
/// The slot in the entry stores the (pointer, size) pair of the copied payload byte-aligned, which is exactly the form
/// readRecordFields produces for nested var-sized members, so reading an entry repeatedly is idempotent.
VarSizedStoreFn
createStoreVarSized(const BorrowedNautilusBuffer& hashMapBuffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    return
        [&hashMapBuffer, &bufferProvider](
            const nautilus::val<int8_t*>& slot, const nautilus::val<int8_t*> varSizedContent, const nautilus::val<uint64_t>& varSizedSize)
    {
        const auto copiedPayload = nautilus::invoke(
            +[](const TupleBuffer* tupleBuffer, AbstractBufferProvider* provider, const uint64_t size) -> int8_t*
            {
                auto chm = ChainedHashMap::load(*tupleBuffer);
                const auto space = chm.allocateSpaceForVarSized(provider, size);
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): the page hands out std::byte spans by design.
                return reinterpret_cast<int8_t*>(space.data());
            },
            hashMapBuffer.asArg(),
            bufferProvider,
            varSizedSize);
        nautilus::memcpy(copiedPayload, varSizedContent, varSizedSize);
        VarVal{VariableSizedData{copiedPayload, varSizedSize}, false, false}.writeToMemory(slot);
    };
}

VarSizedLoadFn createLoadVarSized()
{
    return [](const nautilus::val<int8_t*> slot) -> std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
    {
        const auto payload = VarVal::readVarValFromMemory(slot, DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, false)
                                 .getRawValueAs<VariableSizedData>();
        return {payload.getContent(), payload.getSize()};
    };
}

FieldAccess createFieldAccess(const nautilus::val<ChainedHashMapEntry*>& entryRef, const FieldOffsets& field)
{
    const auto& [fieldIdentifier, type, fieldOffset] = field;
    const auto castedEntryAddress = static_cast<nautilus::val<int8_t*>>(entryRef);
    return FieldAccess{.name = fieldIdentifier, .dataType = type, .address = castedEntryAddress + fieldOffset};
}
}

VarVal ChainedEntryMemoryProvider::readVarVal(
    const nautilus::val<ChainedHashMapEntry*>& entryRef, const Record::RecordFieldIdentifier& fieldName) const
{
    for (const auto& field : nautilus::static_iterable(fields))
    {
        if (field.fieldIdentifier == fieldName)
        {
            const auto record = readRecordFields({createFieldAccess(entryRef, field)}, createLoadVarSized());
            return record.read(fieldName);
        }
    }
    throw FieldNotFound("Field {} not found in ChainedEntryMemoryProvider", fieldName);
}

Record ChainedEntryMemoryProvider::readRecord(const nautilus::val<ChainedHashMapEntry*>& entryRef) const
{
    const auto fieldAccesses
        = FieldAccess::createFieldAccesses(fields, [&entryRef](const FieldOffsets& field) { return createFieldAccess(entryRef, field); });
    return readRecordFields(fieldAccesses, createLoadVarSized());
}

void ChainedEntryMemoryProvider::writeRecord(
    const nautilus::val<ChainedHashMapEntry*>& entryRef,
    const BorrowedNautilusBuffer& hashMapBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const Record& record) const
{
    const auto fieldAccesses
        = FieldAccess::createFieldAccesses(fields, [&entryRef](const FieldOffsets& field) { return createFieldAccess(entryRef, field); });
    writeRecordFields(fieldAccesses, record, createStoreVarSized(hashMapBuffer, bufferProvider));
}

void ChainedEntryMemoryProvider::writeEntryRef(
    const nautilus::val<ChainedHashMapEntry*>& entryRef,
    const BorrowedNautilusBuffer& hashMapBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<ChainedHashMapEntry*>& otherEntryRef) const
{
    writeRecord(entryRef, hashMapBuffer, bufferProvider, readRecord(otherEntryRef));
}

std::vector<Record::RecordFieldIdentifier> ChainedEntryMemoryProvider::getAllFieldIdentifiers() const
{
    std::vector<Record::RecordFieldIdentifier> fieldIdentifiers;
    fieldIdentifiers.reserve(fields.size());
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
