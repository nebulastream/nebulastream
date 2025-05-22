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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/val_ptr.hpp>
#include <val_concepts.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES::Nautilus::Util
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

inline std::unordered_map<BasicPhysicalType::NativeType, std::function<VarVal(const VarVal&, const nautilus::val<int8_t*>&)>>
    storeValueFunctionMap = {
        {BasicPhysicalType::NativeType::BOOLEAN,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal booleanValue = value.cast<nautilus::val<bool>>();
             booleanValue.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::INT_8,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int8Value = value.cast<nautilus::val<int8_t>>();
             int8Value.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::INT_16,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int16Value = value.cast<nautilus::val<int16_t>>();
             int16Value.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::INT_32,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int32Value = value.cast<nautilus::val<int32_t>>();
             int32Value.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::INT_64,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal int64Value = value.cast<nautilus::val<int64_t>>();
             int64Value.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::UINT_8,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint8Value = value.cast<nautilus::val<uint8_t>>();
             uint8Value.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::UINT_16,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint16Value = value.cast<nautilus::val<uint16_t>>();
             uint16Value.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::UINT_32,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint32Value = value.cast<nautilus::val<uint32_t>>();
             uint32Value.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::UINT_64,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal uint64Value = value.cast<nautilus::val<uint64_t>>();
             uint64Value.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::FLOAT,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal floatValue = value.cast<nautilus::val<float>>();
             floatValue.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::DOUBLE,
         [](const VarVal& value, const nautilus::val<int8_t*>& memoryReference)
         {
             const VarVal doubleValue = value.cast<nautilus::val<double>>();
             doubleValue.writeToMemory(memoryReference);
             return value;
         }},
        {BasicPhysicalType::NativeType::UNDEFINED, nullptr},
};

}
