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
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Reads a single field value from @param address, honoring the leading null-byte convention. Non-VARSIZED
/// values are read directly; for VARSIZED the (pointer, length) pair is resolved by @param loadVarSized.
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
    if (dataType.type != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(varValRef, dataType, null);
    }
    const auto [ptr, len] = loadVarSized(varValRef);
    return VarVal{VariableSizedData{ptr, len}, dataType.nullable, null};
}

/// Writes a single field value to @param address, honoring the leading null-byte convention. Non-VARSIZED
/// values go through storeValueFunctionMap; for VARSIZED the payload is stored by @param storeVarSized.
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
    if (dataType.type != DataType::Type::VARSIZED)
    {
        /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16.
        /// We get the correct function to call via a unordered_map
        if (const auto storeFunction = storeValueFunctionMap.find(dataType.type); storeFunction != storeValueFunctionMap.end())
        {
            storeFunction->second(value, addressToWriteValue);
            return;
        }
        throw UnknownDataType("Physical Type: {} is currently not supported", dataType);
    }
    storeVarSized(addressToWriteValue, value);
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
