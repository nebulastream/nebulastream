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

#include <algorithm>
#include <any>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DataTypes/VarVal.hpp>
#include <nautilus/Engine.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

#include <rapidcheck.h> /// NOLINT(misc-include-cleaner)

namespace NES::TestUtils
{

/// NOLINTBEGIN(misc-include-cleaner, bugprone-unchecked-optional-access)

enum class EngineMode : std::uint8_t
{
    Interpreter,
    Compiler
};

/// Builds a NautilusEngine configured for either interpreted or compiled execution (mlir/legacy backend).
nautilus::engine::NautilusEngine makeEngine(EngineMode mode);

/// Cap upfront pool allocation so a large buffer size doesn't allocate gigabytes.
constexpr uint64_t MAX_POOL_BYTES = 64ULL * 1024 * 1024;

/// Maximum number of fields generated per random schema.
constexpr size_t MAX_SCHEMA_FIELDS = 128;

/// Maximum number of buffers pre-allocated for a test's buffer provider, scaled down for large buffer sizes.
constexpr size_t POOLED_BUFFER_COUNT = 4096;

/// Floor on the pool-buffer count so even a large buffer-size pool entry leaves room for the varsized child path.
constexpr size_t MIN_POOLED_BUFFER_COUNT = 16;

/// Computes a pool-buffer count that scales inversely with bufferSize so that upfront allocation
/// stays bounded by MAX_POOL_BYTES while still leaving headroom for the varsized child path.
constexpr size_t pooledBufferCountFor(uint64_t bufferSize)
{
    const auto fitsInBudget = MAX_POOL_BYTES / bufferSize;
    return std::clamp<size_t>(fitsInBudget, MIN_POOLED_BUFFER_COUNT, POOLED_BUFFER_COUNT);
}

/// Range bounds for randomly sized VARSIZED payloads. Lower bound is the first printable ASCII char,
/// upper bound is the last; we keep payloads as printable ASCII so failing inputs are easy to read.
constexpr char PRINTABLE_ASCII_MIN = 32;
constexpr char PRINTABLE_ASCII_MAX = 127;
constexpr size_t MAX_VARSIZED_LEN = 64;

/// Upper bound (exclusive) on the per-property generated record count.
constexpr uint64_t MAX_ITEMS_PER_PROPERTY = 501;

using AnyVec = std::vector<std::any>;

/// Value types used by the property generators (includes VARSIZED).
constexpr std::array ALL_VALUE_TYPES = {
    DataType::Type::UINT8,
    DataType::Type::UINT16,
    DataType::Type::UINT32,
    DataType::Type::UINT64,
    DataType::Type::INT8,
    DataType::Type::INT16,
    DataType::Type::INT32,
    DataType::Type::INT64,
    DataType::Type::FLOAT32,
    DataType::Type::FLOAT64,
    DataType::Type::VARSIZED,
};

/// Builds a Schema with sequentially named fields ("field0", "field1", ...) from the given DataType vector.
Schema<QualifiedUnboundField, Ordered> createSchemaFromDataTypes(const std::vector<DataType>& dataTypes);

/// Generator for a non-empty vector of DataType drawn from a Type pool.
/// Nullability is randomised per field.
rc::Gen<std::vector<DataType>> genDataTypeSchema(std::span<const DataType::Type> pool, size_t minFields, size_t maxFields);

template <typename T>
std::any genScalarAny(bool nullable)
{
    if (nullable)
    {
        const bool isNull = *rc::gen::arbitrary<bool>();
        if (isNull)
        {
            return std::any{std::optional<T>{}};
        }
        return std::any{std::optional<T>{*rc::gen::arbitrary<T>()}};
    }
    return std::any{*rc::gen::arbitrary<T>()};
}

/// Generator for a record of arbitrary values matching the given field types.
rc::Gen<AnyVec> genAnyVec(std::vector<DataType> types);

template <typename T>
int compareTyped(const std::any& lhs, const std::any& rhs, const bool nullable)
{
    if (nullable)
    {
        const auto left = std::any_cast<std::optional<T>>(lhs);
        const auto right = std::any_cast<std::optional<T>>(rhs);
        if (left < right)
        {
            return -1;
        }
        if (left > right)
        {
            return 1;
        }
    }
    else
    {
        const auto left = std::any_cast<T>(lhs);
        const auto right = std::any_cast<T>(rhs);
        if (left < right)
        {
            return -1;
        }
        if (left > right)
        {
            return 1;
        }
    }
    return 0;
}

int compareAnyField(const std::any& lhs, const std::any& rhs, DataType type);

bool anyVecsEqual(const AnyVec& lhs, const AnyVec& rhs, const std::vector<DataType>& types);

/// Trace-time helpers: each template instantiation produces a distinct function pointer,
/// letting the compiled lambdas pull/push typed values through nautilus::invoke callbacks.
template <typename T>
nautilus::val<T> fetchScalarFromAnyVec(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, bool nullable)
{
    if (nullable)
    {
        return nautilus::invoke(
            +[](AnyVec* anyVec, uint64_t pos) -> T
            {
                const auto& opt = std::any_cast<const std::optional<T>&>((*anyVec)[pos]);
                return opt.value_or(T{});
            },
            rec,
            nautilus::val<uint64_t>{fieldIdx});
    }
    return nautilus::invoke(
        +[](AnyVec* anyVec, uint64_t pos) -> T { return std::any_cast<T>((*anyVec)[pos]); }, rec, nautilus::val<uint64_t>{fieldIdx});
}

template <typename T>
void storeScalarToAnyVec(
    const nautilus::val<AnyVec*>& out, uint64_t fieldIdx, const nautilus::val<T>& value, bool nullable, const nautilus::val<bool>& isNull)
{
    if (nullable)
    {
        nautilus::invoke(
            +[](AnyVec* anyVec, uint64_t pos, T val, bool null)
            { (*anyVec)[pos] = null ? std::any{std::optional<T>{}} : std::any{std::optional<T>{val}}; },
            out,
            nautilus::val<uint64_t>{fieldIdx},
            value,
            isNull);
        return;
    }
    nautilus::invoke(
        +[](AnyVec* anyVec, uint64_t pos, T scalar) { (*anyVec)[pos] = std::any{scalar}; }, out, nautilus::val<uint64_t>{fieldIdx}, value);
}

void storeVarValToAnyVec(const nautilus::val<AnyVec*>& out, uint64_t pos, const VarVal& value, const DataType& dataType);

nautilus::val<bool> checkIfNullInAnyVec(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, DataType::Type type);

nautilus::val<AnyVec*> anyVecPushBack(const nautilus::val<std::vector<AnyVec>*>& vec, const nautilus::val<size_t>& numberOfFields);

VarVal buildVarVal(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, DataType dataType);

/// NOLINTEND(misc-include-cleaner, bugprone-unchecked-optional-access)

}
