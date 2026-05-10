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
#include <Interface/RecordLayoutUtil.hpp>

#include <cstdint>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val_bool.hpp>
#include <val_memcpy.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Used for STRUCT and FIXEDSIZED values, which may contain non-nested varsized elements.
/// Varsized values are stored as 16 bytes inside the struct data. The first 8 bytes describe the address of the values and the last 8 bytes describe the size of the content.
/// Since between nodes, varsized values are stored as offset (4 bytes), child index (4 bytes), and size (8 bytes), we need to overwrite these 16 byts with the type of reference
/// described above, which we do in this function.
void convertVarsizedReferences(const DataType& dataType, const nautilus::val<int8_t*>& address, const VarSizedLoadFn& loadVarSized)
{
    nautilus::val<int8_t*> varValRef = address;
    /// Early opt out in case the data type is flat, so all basic types and structs / fixedsized values without varsized elements.
    if (!dataType.isFlat())
    {
        switch (dataType.type)
        {
            case DataType::Type::FIXEDSIZED: {
                /// Iterate over elements and convert varsized references.
                for (nautilus::static_val<uint32_t> i = 0; i < dataType.count; ++i)
                {
                    convertVarsizedReferences(
                        *dataType.elementType,
                        varValRef + nautilus::val<size_t>{i * dataType.elementType->getSizeInBytesWithoutNull()},
                        loadVarSized);
                }
                return;
            }
            case DataType::Type::STRUCT: {
                for (nautilus::static_val<size_t> i = 0; i < dataType.fields.size(); ++i)
                {
                    /// Iterate over fields and convert varsized references.
                    const auto& [field, type] = dataType.fields.at(i);
                    convertVarsizedReferences(type, varValRef, loadVarSized);
                    varValRef += nautilus::val<size_t>{type.getSizeInBytesWithoutNull()};
                }
                return;
            }
            case DataType::Type::VARSIZED: {
                /// Load variablesized access as pointer and size and write these values byte-aligned into varValRef.
                const auto [ptr, size] = loadVarSized(varValRef);
                const VarVal variableSizedVal{VariableSizedData{ptr, size}, false, false};
                variableSizedVal.writeToMemory(varValRef);
                return;
            }
            default: {
                /// Santity check. The other datatypes should never be deemed as not fixed sized.
                INVARIANT(false, "Type {} was deemed as not fixed-sized!", magic_enum::enum_name(dataType.type));
                return;
            }
        }
    }
}

/// Reads a single field value from @param address, honoring the leading null-byte convention. Non-VARSIZED
/// values are read directly; for VARSIZED the (pointer, length) pair is resolved by @param loadVarSized.
/// STRUCT and FIXEDSIZED directly store the pointer to the address in their values, but will convert any contained varsized fields into (pointer, length) form first.
VarVal readFieldValue(const DataType& dataType, const nautilus::val<int8_t*>& address, const VarSizedLoadFn& loadVarSized)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<bool> null = false;
    nautilus::val<int8_t*> varValRef = address;
    if (dataType.nullable)
    {
        /// Reading the first byte (null) and then incrementing the memref by 1 byte to read the actual value
        null = readValueFromMemRef<bool>(address);
        varValRef += 1;
    }
    if (dataType.type == DataType::Type::STRUCT)
    {
        /// Inline storage: the struct's bytes live directly in the tuple at `varValRef`. Per-field offsets are determined by `StructData`'s inline-layout rules.
        /// We need to convert the the varsized references into (pointer, length) form first.
        convertVarsizedReferences(dataType, varValRef, loadVarSized);
        return VarVal{StructData{varValRef, dataType.fields}, dataType.nullable, null};
    }
    if (dataType.type == DataType::Type::FIXEDSIZED)
    {
        /// Like struct, we first convert the varsized elements and then directly store the pointer to the memory in the value.
        convertVarsizedReferences(dataType, varValRef, loadVarSized);
        return VarVal{FixedSizedData{varValRef, dataType.count, *dataType.elementType}, dataType.nullable, null};
    }
    if (dataType.type == DataType::Type::VARSIZED)
    {
        const auto [ptr, len] = loadVarSized(varValRef);
        return VarVal{VariableSizedData{ptr, len}, dataType.nullable, null};
    }
    return VarVal::readVarValFromMemory(varValRef, dataType, null);
}

/// Writes a single field value to @param address, honoring the leading null-byte convention. Non-VARSIZED
/// values go through storeValueFunctionMap; for VARSIZED the payload is stored by @param storeVarSized.
/// FIXEDSIZED and STRUCT are store their members inline. How many copy operations are used depends on whether they contain varsized elements, which need to be stored as reference into a child buffer.
void writeFieldValue(
    const DataType& dataType, const nautilus::val<int8_t*>& address, const VarVal& value, const VarSizedStoreFn& storeVarSized)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<int8_t*> addressToWriteValue = address;
    if (dataType.nullable)
    {
        /// Writing the null value to the first byte and then incrementing the memref by 1 byte to store the actual value
        VarVal{value.isNull()}.writeToMemory(addressToWriteValue);
        addressToWriteValue += 1;
    }
    if (dataType.type == DataType::Type::STRUCT)
    {
        const auto src = value.getRawValueAs<StructData>();
        if (dataType.isFlat())
        {
            /// Store struct via a single memcpy operation, since all values can be stored bytealigned
            nautilus::memcpy(addressToWriteValue, src.getRawPtr(), src.getTotalSizeInBytes());
        }
        else
        {
            /// Write element per element, writing varsized elements into a child-buffer and inlining the variablesized-access data (ptr + size as 16 bytes)
            /// This holds potential for further optimization: Fixedsized fields within a struct could be reordered to be contained next to each other.
            /// These elements are copied into the tuple buffer via one memcpy. Afterwards, only the variable-sized fields need to be written seperatly.
            for (nautilus::static_val<uint64_t> i = 0; i < src.getNumFields(); ++i)
            {
                const auto& [field, type] = src.getFields().at(i);
                writeFieldValue(type, addressToWriteValue, src.at(i), storeVarSized);
                addressToWriteValue += nautilus::val<size_t>{type.getSizeInBytesWithoutNull()};
            }
        }
        return;
    }
    if (dataType.type == DataType::Type::FIXEDSIZED)
    {
        /// Treated like struct and inlines its values
        const auto src = value.getRawValueAs<FixedSizedData>();
        if (dataType.isFlat())
        {
            nautilus::memcpy(addressToWriteValue, src.getRawPtr(), src.getTotalSizeInBytes());
        }
        else
        {
            for (nautilus::static_val<size_t> i = 0; i < src.getNumElements(); ++i)
            {
                writeFieldValue(src.getElementType(), addressToWriteValue, src.at(nautilus::val<uint64_t>{i}), storeVarSized);
                addressToWriteValue += nautilus::val<size_t>{dataType.elementType->getSizeInBytesWithoutNull()};
            }
        }
        return;
    }
    if (dataType.type == DataType::Type::VARSIZED)
    {
        const auto src = value.getRawValueAs<VariableSizedData>();
        storeVarSized(addressToWriteValue, src.getContent(), src.getSize());
        return;
    }
    /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16.
    /// We get the correct function to call via a unordered_map
    if (const auto storeFunction = storeValueFunctionMap.find(dataType.type); storeFunction != storeValueFunctionMap.end())
    {
        storeFunction->second(value, addressToWriteValue);
        return;
    }
    throw UnknownDataType("Physical Type: {} is currently not supported", dataType);
}
}

Record readRecordFields(const std::vector<FieldAccess>& fields, const VarSizedLoadFn& loadVarSized)
{
    Record record;
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, dataType, address] = fields.at(i);
        record.write(name, readFieldValue(dataType, address, loadVarSized));
    }
    return record;
}

void writeRecordFields(const std::vector<FieldAccess>& fields, const Record& record, const VarSizedStoreFn& storeVarSized)
{
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, dataType, address] = fields.at(i);
        if (record.hasField(name))
        {
            writeFieldValue(dataType, address, record.read(name), storeVarSized);
        }
    }
}

}
