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
#include <DataTypes/VarSizedLoader.hpp>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/FixedSizedData.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <DataTypes/VectorData.hpp>
#include <nautilus/val.hpp>

namespace NES
{

VarSizedLoadFn inlineVarSizedLoader()
{
    return [](const nautilus::val<int8_t*> slot) -> std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
    {
        const nautilus::val<int8_t*> ptr = readValueFromMemRef<int8_t*>(slot);
        const nautilus::val<uint64_t> size = readValueFromMemRef<uint64_t>(slot + nautilus::val<size_t>(sizeof(int8_t*)));
        return {ptr, size};
    };
}

VarVal readNestedVarVal(const nautilus::val<int8_t*>& address, const DataType& type, const VarSizedLoadFn& loadVarSized)
{
    switch (type.type)
    {
        case DataType::Type::VARSIZED: {
            const auto [ptr, size] = loadVarSized(address);
            return {VariableSizedData{ptr, size}, false, false};
        }
        case DataType::Type::VECTOR: {
            const auto [ptr, size] = loadVarSized(address);
            return {VectorData{ptr, *type.elementType, size}, false, false};
        }
        case DataType::Type::FIXEDSIZED: {
            return {FixedSizedData{address, type.count, *type.elementType, loadVarSized}, false, false};
        }
        case DataType::Type::STRUCT: {
            return {StructData{address, type.fields, loadVarSized}, false, false};
        }
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::INT8:
        case DataType::Type::UINT8:
        case DataType::Type::INT16:
        case DataType::Type::UINT16:
        case DataType::Type::INT32:
        case DataType::Type::UINT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
        case DataType::Type::UNDEFINED: {
            return VarVal::readNonNullableVarValFromMemory(address, type);
        }
    }
}

}
