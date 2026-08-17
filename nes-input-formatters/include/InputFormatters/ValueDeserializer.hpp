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

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// The one result shape every deserializer produces, so that all of them share a single nautilus signature.
/// A nautilus function may only return a 'nautilus::val<T>', and a VarVal is a std::variant plus a plain bool, so
/// it cannot cross that boundary -- a pointer to this POD can. Fixed-size types occupy 'value', VARSIZED the pair.
struct DeserializedValue
{
    alignas(8) std::array<std::byte, 8> value;
    int8_t* varsizedPtr;
    uint64_t varsizedSize;
    bool isNull;
};

/// The runtime function a plugin contributes. Plain C++: everything the deserializer does happens here, where it
/// costs no trace operations. 'nullValues' and 'arena' are arguments rather than captures because the compiled
/// code outlives the tracing run.
/// fieldAddress == nullptr and fieldSize == 0 means the field was not found. A deserializer that cannot view into
/// the raw buffer, because it has to decode first, allocates from the arena, which is reclaimed after the pipeline
/// invocation -- that is, after the record has been written into a tuple buffer.
using DeserializeProxy = DeserializedValue* (*)(int8_t*, uint64_t, const std::vector<std::string>*, Arena*);

/// The DataType a proxy writing a C++ T reports as its produced type.
template <typename T>
constexpr DataType::Type dataTypeOf()
{
    if constexpr (std::is_same_v<T, bool>)
    {
        return DataType::Type::BOOLEAN;
    }
    else if constexpr (std::is_same_v<T, char>)
    {
        return DataType::Type::CHAR;
    }
    else if constexpr (std::is_same_v<T, int8_t>)
    {
        return DataType::Type::INT8;
    }
    else if constexpr (std::is_same_v<T, int16_t>)
    {
        return DataType::Type::INT16;
    }
    else if constexpr (std::is_same_v<T, int32_t>)
    {
        return DataType::Type::INT32;
    }
    else if constexpr (std::is_same_v<T, int64_t>)
    {
        return DataType::Type::INT64;
    }
    else if constexpr (std::is_same_v<T, uint8_t>)
    {
        return DataType::Type::UINT8;
    }
    else if constexpr (std::is_same_v<T, uint16_t>)
    {
        return DataType::Type::UINT16;
    }
    else if constexpr (std::is_same_v<T, uint32_t>)
    {
        return DataType::Type::UINT32;
    }
    else if constexpr (std::is_same_v<T, uint64_t>)
    {
        return DataType::Type::UINT64;
    }
    else if constexpr (std::is_same_v<T, float>)
    {
        return DataType::Type::FLOAT32;
    }
    else
    {
        return DataType::Type::FLOAT64;
    }
}

/// Base class for all value deserializers. A plugin does not emit traced code itself; it names a runtime function
/// and hands it over. The base traces it once per pipeline and calls it thereafter, so a schema with many fields
/// of one type pays for one instantiation rather than one per column.
class ValueDeserializer
{
public:
    virtual ~ValueDeserializer() noexcept = default;

    [[nodiscard]] virtual DeserializeProxy proxy() const = 0;

    /// Called once from the pipeline's setup(), so that the per-field path below is a pointer compare rather than a
    /// name hash under a read lock.
    void resolve(CompilationContext& compilationContext) { tracedFunction.resolve(compilationContext, tracedFunctionName, proxy()); }

    /// The type the proxy writes into DeserializedValue, which is NOT necessarily the field's type: VALUE_DESERIALIZERS
    /// may point a FLOAT64 field at a FLOAT32 deserializer to read it at lower precision. The result has to be read back
    /// at the width it was written, and the VarVal then widens to the field's type where it is stored.
    [[nodiscard]] virtual DataType::Type producedType() const = 0;

    /// Calls the plugin's shared function and rebuilds a VarVal from its result: one call plus one or two loads
    /// per field, whatever the deserializer does internally.
    [[nodiscard]] VarVal deserializeToVarVal(
        CompilationContext& compilationContext,
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const DataType& fieldType) const
    {
        auto& deserialize = tracedFunction.get(compilationContext, tracedFunctionName, proxy());
        const auto result
            = deserialize(fieldAddress, fieldSize, nautilus::val<const std::vector<std::string>*>{&nullValues}, arena.getArena());

        /// For a non-nullable field the flag is a trace-time constant, so the load is skipped entirely.
        const nautilus::val<bool> isNull
            = fieldType.nullable ? *getMemberWithOffset<bool>(result, offsetof(DeserializedValue, isNull)) : nautilus::val<bool>{false};

        /// readVarValFromMemory() dispatches on the DataType in plain C++, so no per-type switch reappears here
        /// and no branch is traced. It does not handle VARSIZED, which needs the pointer/size pair.
        if (producedType() == DataType::Type::VARSIZED)
        {
            const VariableSizedData varsized{
                *getMemberWithOffset<int8_t*>(result, offsetof(DeserializedValue, varsizedPtr)),
                *getMemberWithOffset<uint64_t>(result, offsetof(DeserializedValue, varsizedSize))};
            return VarVal{varsized, fieldType.nullable, isNull};
        }
        const DataType producedDataType{
            producedType(), fieldType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE};
        return VarVal::readVarValFromMemory(
            getMemberWithOffset<int8_t>(result, offsetof(DeserializedValue, value)), producedDataType, isNull);
    }

protected:
    /// Identity of the traced function, composed once. Nautilus interns by name and silently collapses two bodies
    /// sharing one, so the name MUST encode every value the proxy bakes in: nullability, quoting, trailing-space
    /// handling. The prefix keeps plugin names clear of the ones the pipeline and its operators register.
    /// Composed at construction because this is read per field per record.
    explicit ValueDeserializer(const std::string_view tracedName)
        : tracedFunctionName(std::string{"ValueDeserializer::"}.append(tracedName))
    {
    }

    const std::string tracedFunctionName;

private:
    TracedInvokeMemo<DeserializeProxy> tracedFunction;
};
}
