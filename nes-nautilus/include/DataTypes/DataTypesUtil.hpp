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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Get member returns the MemRef to a specific class member as an offset to a objectReference.
/// This is taken from https://stackoverflow.com/a/20141143 and modified to work with a nautilus::val<int8_t*>
/// This does not work with multiple inheritance, for example, https://godbolt.org/z/qzExEd
template <typename T, typename U>
nautilus::val<int8_t*> getMemberRef(nautilus::val<int8_t*> objectReference, U T::* member)
{
#pragma GCC diagnostic ignored "-Wnull-pointer-subtraction"
    return objectReference + ((char*)&((T*)nullptr->*member) - (char*)(nullptr)); /// NOLINT
}

template <typename T>
static nautilus::val<T*> getMemberWithOffset(nautilus::val<int8_t*> objectReference, const size_t memberOffset)
{
#pragma GCC diagnostic ignored "-Wnull-pointer-subtraction"
    return static_cast<nautilus::val<T*>>(objectReference + memberOffset); /// NOLINT
}

template <typename T>
static nautilus::val<T**> getMemberPtrWithOffset(nautilus::val<T*> objectReference, const size_t memberOffset)
{
#pragma GCC diagnostic ignored "-Wnull-pointer-subtraction"
    return static_cast<nautilus::val<T**>>(objectReference + memberOffset); /// NOLINT
}

template <typename T>
nautilus::val<T> readValueFromMemRef(const nautilus::val<int8_t*>& memRef)
{
    return static_cast<nautilus::val<T>>(*static_cast<nautilus::val<T*>>(memRef));
}

inline const std::unordered_map<DataType::Type, std::function<VarVal(const VarVal&, const nautilus::val<int8_t*>&)>> storeValueFunctionMap
    = {
        {DataType::Type::BOOLEAN,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal booleanValue{value.getRawValueAs<nautilus::val<bool>>()};
             booleanValue.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::INT8,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int8Value{value.getRawValueAs<nautilus::val<int8_t>>()};
             int8Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::INT16,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int16Value{value.getRawValueAs<nautilus::val<int16_t>>()};
             int16Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::INT32,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int32Value{value.getRawValueAs<nautilus::val<int32_t>>()};
             int32Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::INT64,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int64Value{value.getRawValueAs<nautilus::val<int64_t>>()};
             int64Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::CHAR,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint8Value{value.getRawValueAs<nautilus::val<char>>()};
             uint8Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UINT8,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint8Value{value.getRawValueAs<nautilus::val<uint8_t>>()};
             uint8Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UINT16,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint16Value{value.getRawValueAs<nautilus::val<uint16_t>>()};
             uint16Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UINT32,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint32Value{value.getRawValueAs<nautilus::val<uint32_t>>()};
             uint32Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UINT64,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint64Value{value.getRawValueAs<nautilus::val<uint64_t>>()};
             uint64Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::FLOAT32,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal floatValue{value.getRawValueAs<nautilus::val<float>>()};
             floatValue.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::FLOAT64,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal doubleValue{value.getRawValueAs<nautilus::val<double>>()};
             doubleValue.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UNDEFINED, nullptr},
};

/// Resolves a VARSIZED field's payload from its slot, returning (pointer, size). Supplied per container.
using LoadVarSized = std::function<std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>(nautilus::val<int8_t*> fieldSlot)>;

/// Copies size bytes from data into container owned storage and records the location in fieldSlot.
using StoreVarSized = std::function<void(nautilus::val<int8_t*> fieldSlot, nautilus::val<int8_t*> data, nautilus::val<uint64_t> size)>;

/// Reads a field in our standard layout: an optional null byte followed by the value.
inline VarVal loadFieldValue(const DataType& dataType, const nautilus::val<int8_t*>& fieldAddress, const LoadVarSized& loadVarSized)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<bool> null = false;
    nautilus::val<int8_t*> valueAddress = fieldAddress;
    if (dataType.nullable)
    {
        /// Reading the first byte (null) and then incrementing the memref by 1 byte to read the actual value
        null = readValueFromMemRef<bool>(fieldAddress);
        valueAddress += 1;
    }

    if (dataType.type != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(valueAddress, dataType, null);
    }

    const auto [content, size] = loadVarSized(valueAddress);
    return VarVal{VariableSizedData(content, size), dataType.nullable, null};
}

/// Writes a field in our standard layout: an optional null byte followed by the value.
inline void storeFieldValue(
    const DataType& dataType, const nautilus::val<int8_t*>& fieldAddress, const VarVal& value, const StoreVarSized& storeVarSized)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<int8_t*> valueAddress = fieldAddress;
    if (dataType.nullable)
    {
        /// Writing the null value to the first byte and then incrementing the memref by 1 byte to store the actual value
        VarVal{value.isNull()}.writeToMemory(valueAddress);
        valueAddress += 1;
    }

    if (dataType.type != DataType::Type::VARSIZED)
    {
        /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16
        /// We get the correct function to call via a unordered_map
        const auto storeFunction = storeValueFunctionMap.find(dataType.type);
        if (storeFunction == storeValueFunctionMap.end())
        {
            throw UnknownDataType("Physical Type: {} is currently not supported", dataType);
        }
        std::ignore = storeFunction->second(value, valueAddress);
        return;
    }

    const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
    storeVarSized(valueAddress, varSizedValue.getContent(), varSizedValue.getSize());
}

}
