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
#include <cstring>
#include <functional>
#include <unordered_map>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/inline.hpp>
#include <nautilus/val_ptr.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Byte equality with an ALWAYS-INLINED, call-free body: fused into the JIT module it is pure
/// IR, so for a constant-global RHS (an embedConstantBytes'd query literal) with compile-time
/// size LLVM unrolls and folds it to immediate compares (WHERE state = 'az' becomes two icmps).
/// The loads go through std::memcpy, which lowers to the @llvm.memcpy INTRINSIC (name preserved
/// through bitcode extraction) -- do NOT use std::memcmp here: it has no intrinsic, and the
/// inlining pass's symbol resolution renames external callees to their runtime addresses, which
/// destroys the libcall identity LLVM's memcmp expansion keys on (nautilus::memcmp has the same
/// problem, plus worst-case memory(readwrite) attributes that block reordering entirely).
NAUTILUS_INLINE inline bool bytesEqual(const int8_t* lhs, const int8_t* rhs, const uint64_t size)
{
    uint64_t index = 0;
    for (; index + 8 <= size; index += 8)
    {
        uint64_t lhsWord = 0;
        uint64_t rhsWord = 0;
        std::memcpy(&lhsWord, lhs + index, 8);
        std::memcpy(&rhsWord, rhs + index, 8);
        if (lhsWord != rhsWord)
        {
            return false;
        }
    }
    for (; index < size; ++index)
    {
        if (lhs[index] != rhs[index])
        {
            return false;
        }
    }
    return true;
}

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

}
