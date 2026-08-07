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
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <Runtime/BufferManager.hpp>
#include <nautilus/Engine.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

/// Umbrella header: rapidcheck spreads rc::gen and the DefaultArbitrary specialisations over impl headers
/// (gen/*.hpp) that cannot be included directly, so the umbrella is the only supported entry point.
#include <rapidcheck.h>

namespace NES::TestUtils
{

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

/// Sizes the test buffer manager so its unpooled-memory allowance can absorb the generated variable-sized payloads
/// and the allocator's rolling-average chunk preallocation.
constexpr size_t pooledBufferCountFor(uint64_t bufferSize, uint64_t varSizedMemoryBudget)
{
    const auto requiredForVarSized = ((8 * varSizedMemoryBudget) + (9 * bufferSize) - 1) / (9 * bufferSize);
    return std::max<size_t>(pooledBufferCountFor(bufferSize), requiredForVarSized);
}

/// Creates a BufferManager sized to hold at least numberOfBuffers pooled buffers of bufferSize bytes each.
/// Wraps BufferManager::create's total-memory-budget signature: reserving 90% of the budget for unpooled
/// buffers and sizing the total budget at 10x the requested pooled bytes leaves exactly numberOfBuffers
/// pooled buffers after the unpooled share is carved out, with headroom to spare for unpooled allocations.
std::shared_ptr<BufferManager> createBufferManager(uint64_t bufferSize, uint64_t numberOfBuffers);

/// Range bounds for randomly sized VARSIZED payloads. Lower bound is the first printable ASCII char,
/// upper bound is the last; we keep payloads as printable ASCII so failing inputs are easy to read.
constexpr char PRINTABLE_ASCII_MIN = 32;
constexpr char PRINTABLE_ASCII_MAX = 127;

/// Upper bound (exclusive) on the per-property generated record count.
constexpr uint64_t MAX_ITEMS_PER_PROPERTY = 501;

using AnyVec = std::vector<std::any>;
using VarSizedMemoryBudget = std::shared_ptr<uint64_t>;

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
rc::Gen<std::vector<DataType>> genDataTypeSchema(std::span<const DataType::Type> dataTypesPool, size_t minFields, size_t maxFields);

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

/// Generator for a record of arbitrary values matching the given field types. Generated VARSIZED payload lengths
/// are drawn from and subtracted from the shared remaining memory budget.
rc::Gen<AnyVec> genAnyVec(std::vector<DataType> types, VarSizedMemoryBudget varSizedMemoryBudget);

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

template <typename T>
size_t hashTyped(const std::any& value, const bool nullable)
{
    if (nullable)
    {
        const auto& opt = std::any_cast<const std::optional<T>&>(value);
        return opt.has_value() ? std::hash<T>{}(*opt) : std::hash<bool>{}(false);
    }
    return std::hash<T>{}(std::any_cast<const T&>(value));
}

/// Hashes a single field, mirroring compareAnyField's type dispatch so equal fields (per compareAnyField) always hash equal.
size_t hashAnyField(const std::any& value, DataType type);

/// Hashes an AnyVec key field-by-field via hashAnyField, bound to the key schema of the map under test.
struct AnyVecHash
{
    const std::vector<DataType>* keyTypes;

    size_t operator()(const AnyVec& key) const;
};

/// Compares two AnyVec keys field-by-field via compareAnyField, bound to the key schema of the map under test.
struct AnyVecKeyEqual
{
    const std::vector<DataType>* keyTypes;

    bool operator()(const AnyVec& lhs, const AnyVec& rhs) const;
};

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

/// Builds a Record by reading each field via buildVarVal, keyed positionally by fieldNames[i]/fieldTypes[i].
Record buildRecordFromAnyVec(
    const nautilus::val<AnyVec*>& anyVec,
    const std::vector<Record::RecordFieldIdentifier>& fieldNames,
    const std::vector<DataType>& fieldTypes);

/// Stores each field of `record`, keyed positionally by fieldNames[i]/fieldTypes[i], into out[outOffset + i].
void storeRecordToAnyVec(
    const nautilus::val<AnyVec*>& out,
    const Record& record,
    const std::vector<Record::RecordFieldIdentifier>& fieldNames,
    const std::vector<DataType>& fieldTypes,
    uint64_t outOffset);

/// Stores into out[i], i.e. the single-record case.
inline void storeRecordToAnyVec(
    const nautilus::val<AnyVec*>& out,
    const Record& record,
    const std::vector<Record::RecordFieldIdentifier>& fieldNames,
    const std::vector<DataType>& fieldTypes)
{
    storeRecordToAnyVec(out, record, fieldNames, fieldTypes, 0);
}

}
