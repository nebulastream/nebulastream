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
#include <functional>
#include <unordered_map>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/val_ptr.hpp>
#include <val.hpp>
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
nautilus::val<T> readValueFromMemRef(const nautilus::val<int8_t*>& memRef)
{
    return static_cast<nautilus::val<T>>(*static_cast<nautilus::val<T*>>(memRef));
}

inline const std::unordered_map<DataType::Type, std::function<VarVal(const VarVal&, const nautilus::val<int8_t*>&)>> storeValueFunctionMap
    = {
        {DataType::Type::BOOLEAN,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal booleanValue = value.cast<nautilus::val<bool>>();
             booleanValue.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::INT8,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int8Value = value.cast<nautilus::val<int8_t>>();
             int8Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::INT16,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int16Value = value.cast<nautilus::val<int16_t>>();
             int16Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::INT32,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int32Value = value.cast<nautilus::val<int32_t>>();
             int32Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::INT64,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int64Value = value.cast<nautilus::val<int64_t>>();
             int64Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::CHAR,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint8Value = value.cast<nautilus::val<char>>();
             uint8Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UINT8,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint8Value = value.cast<nautilus::val<uint8_t>>();
             uint8Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UINT16,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint16Value = value.cast<nautilus::val<uint16_t>>();
             uint16Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UINT32,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint32Value = value.cast<nautilus::val<uint32_t>>();
             uint32Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UINT64,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint64Value = value.cast<nautilus::val<uint64_t>>();
             uint64Value.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::FLOAT32,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal floatValue = value.cast<nautilus::val<float>>();
             floatValue.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::FLOAT64,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal doubleValue = value.cast<nautilus::val<double>>();
             doubleValue.writeToMemory(memoryReference);
             return value;
         }},
        {DataType::Type::UNDEFINED, nullptr},
};

}
