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
#include <type_traits>
#include <unordered_map>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <nautilus/val_ptr.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Pointers the member-access helpers accept as raw memory: bytes and void, in any constness.
template <typename Pointee>
concept RawMemory
    = std::is_void_v<Pointee> || std::is_same_v<std::remove_cv_t<Pointee>, int8_t> || std::is_same_v<std::remove_cv_t<Pointee>, uint8_t>
    || std::is_same_v<std::remove_cv_t<Pointee>, char> || std::is_same_v<std::remove_cv_t<Pointee>, std::byte>;

/// Get member returns the MemRef to a specific class member as an offset to a objectReference.
/// This is taken from https://stackoverflow.com/a/20141143 and modified to work with a nautilus::val<int8_t*>
/// This does not work with multiple inheritance, for example, https://godbolt.org/z/qzExEd
/// The object reference must point to the member's class or be raw memory; any other pointer needs an explicit cast at the call
/// site, as nautilus, like C++, does not convert between unrelated pointer types implicitly.
template <typename T, typename U, typename Object>
requires std::is_same_v<std::remove_cv_t<Object>, T> || RawMemory<Object>
nautilus::val<int8_t*> getMemberRef(const nautilus::val<Object*>& objectReference, U T::* member)
{
#pragma GCC diagnostic ignored "-Wnull-pointer-subtraction"
    return static_cast<nautilus::val<int8_t*>>(objectReference) + ((char*)&((T*)nullptr->*member) - (char*)(nullptr)); /// NOLINT
}

/// Address of the member at a raw byte offset (e.g. offsetof) into the object. The offset is not tied to the object's type, so any
/// object pointer is accepted.
template <typename T, typename Object>
static nautilus::val<T*> getMemberWithOffset(const nautilus::val<Object*>& objectReference, const size_t memberOffset)
{
    return static_cast<nautilus::val<T*>>(static_cast<nautilus::val<int8_t*>>(objectReference) + memberOffset); /// NOLINT
}

/// Address of the pointer-typed member at a raw byte offset (e.g. offsetof) into the object; see getMemberWithOffset.
template <typename T, typename Object>
static nautilus::val<T**> getMemberPtrWithOffset(const nautilus::val<Object*>& objectReference, const size_t memberOffset)
{
    return static_cast<nautilus::val<T**>>(static_cast<nautilus::val<int8_t*>>(objectReference) + memberOffset); /// NOLINT
}

/// Reads a T from memory that holds a T or is raw memory.
template <typename T, typename Pointee>
requires std::is_same_v<std::remove_cv_t<Pointee>, T> || RawMemory<Pointee>
nautilus::val<T> readValueFromMemRef(const nautilus::val<Pointee*>& memRef)
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

}
