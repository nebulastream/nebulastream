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

#include <algorithm>
#include <any>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed via macros expanded from rapidcheck/gtest.h
#include <nautilus/Engine.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

#include <Util/Ranges.hpp>

#include <rapidcheck.h> /// NOLINT(misc-include-cleaner)

#include <fmt/ranges.h>
#include <rapidcheck/gtest.h>

namespace NES
{

namespace
{
enum class EngineMode : std::uint8_t
{
    Interpreter,
    Compiler
};

nautilus::engine::NautilusEngine makeEngine(EngineMode mode)
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", mode == EngineMode::Compiler);
    options.setOption("mlir.enableMultithreading", false);
    return {options};
}

/// Marker pattern written into freshly handed-out buffers so tests notice if the PagedVector
/// reads uninitialised memory: a dirty page surfaces as 0xDEADBEEF instead of zero.
constexpr uint32_t DIRTY_FILL_PATTERN = 0xDEADBEEF;

struct DirtyBufferProvider : AbstractBufferProvider
{
    explicit DirtyBufferProvider(std::shared_ptr<BufferManager> bm) : bm(std::move(bm)) { }

    static std::shared_ptr<DirtyBufferProvider> create(size_t bufferSize, size_t numberOfBuffers)
    {
        return std::make_shared<DirtyBufferProvider>(BufferManager::create(bufferSize, numberOfBuffers));
    }

    [[nodiscard]] BufferManagerType getBufferManagerType() const override { return bm->getBufferManagerType(); }

    [[nodiscard]] size_t getBufferSize() const override { return bm->getBufferSize(); }

    [[nodiscard]] size_t getNumOfPooledBuffers() const override { return bm->getNumOfPooledBuffers(); }

    [[nodiscard]] size_t getNumOfUnpooledBuffers() const override { return bm->getNumOfUnpooledBuffers(); }

    TupleBuffer getBufferBlocking() override
    {
        /// Tests are single-threaded; nothing releases buffers concurrently, so a blocking get on an exhausted pool would hang.
        /// Fail fast instead so rapidcheck can shrink the offending case quickly.
        auto buffer = bm->getBufferNoBlocking();
        if (!buffer.has_value())
        {
            throw BufferAllocationFailure("DirtyBufferProvider pool exhausted (single-threaded test)");
        }
        for (uint32_t& availableMemoryArea : buffer->getAvailableMemoryArea<uint32_t>())
        {
            availableMemoryArea = DIRTY_FILL_PATTERN;
        }
        return std::move(*buffer);
    }

    std::optional<TupleBuffer> getBufferNoBlocking() override
    {
        auto buffer = bm->getBufferNoBlocking();
        if (buffer)
        {
            for (uint32_t& availableMemoryArea : buffer->getAvailableMemoryArea<uint32_t>())
            {
                availableMemoryArea = DIRTY_FILL_PATTERN;
            }
        }
        return buffer;
    }

    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeoutMs) override
    {
        auto buffer = bm->getBufferWithTimeout(timeoutMs);
        if (buffer)
        {
            for (uint32_t& availableMemoryArea : buffer->getAvailableMemoryArea<uint32_t>())
            {
                availableMemoryArea = DIRTY_FILL_PATTERN;
            }
        }
        return buffer;
    }

    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) override
    {
        auto buffer = bm->getUnpooledBuffer(bufferSize);
        if (buffer)
        {
            for (uint32_t& availableMemoryArea : buffer->getAvailableMemoryArea<uint32_t>())
            {
                availableMemoryArea = DIRTY_FILL_PATTERN;
            }
        }
        return buffer;
    }

    std::shared_ptr<BufferManager> bm;
};

/// Buffer-size pool drawn from per property to exercise both extremes:
/// - tiny buffers (64-512 B) force long page chains, which surfaces correctness issues in the
///   getPageIndex binary search and the last-page cumulative-sum special case;
/// - the large 2 MiB buffer keeps everything on a single page and exercises the no-paging path.
/// Schemas whose tuple size doesn't fit must be discarded via RC_PRE.
constexpr std::array<uint64_t, 4> BUFFER_SIZE_POOL = {128, 512, 4096, 2ULL * 1024 * 1024};

/// Number of buckets pool drawn from property to test different numbers of buckets for the chained hash map.
constexpr std::array<uint64_t, 6> NUM_BUCKETS_POOL = {32, 64, 128, 256, 512, 1024};

/// Number of entries per page — multiplied by entrySize to derive pageSize.
/// Small values (1, 2) force long page chains; large values (64, 512) exercise the bulk path.
constexpr std::array<uint64_t, 6> ENTRIES_PER_PAGE_POOL = {1, 2, 4, 16, 64, 512};

/// Cap upfront pool allocation so the 2 MiB case doesn't allocate gigabytes.
constexpr uint64_t MAX_POOL_BYTES = 64ULL * 1024 * 1024;

/// Maximum number of fields generated per random schema.
constexpr size_t MAX_SCHEMA_FIELDS = 128;

/// Maximum number of buffers pre-allocated for each test's DirtyBufferProvider, scaled down for large buffer sizes.
constexpr size_t POOLED_BUFFER_COUNT = 4096;

/// Floor on the pool-buffer count so even the largest BUFFER_SIZE_POOL entry leaves room for the varsized child path.
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

/// Per-vector item-count range and max number of paged vectors used by the concat properties.
constexpr uint64_t MAX_ITEMS_PER_CONCAT_VECTOR = 201;
constexpr uint64_t MAX_CONCAT_VECTORS = 5;

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
Schema createSchemaFromDataTypes(const std::vector<DataType>& dataTypes)
{
    auto schema = Schema{};

    for (const auto& [typeIdx, dataType] : views::enumerate(dataTypes))
    {
        const auto name = Record::RecordFieldIdentifier("field" + std::to_string(typeIdx));
        schema.addField(name, dataType);
    }
    return schema;
}

/// Generator for a non-empty vector of DataType drawn from a Type pool.
/// Nullability is randomised per field.
rc::Gen<std::vector<DataType>> genDataTypeSchema(std::span<const DataType::Type> pool, size_t minFields, size_t maxFields)
{
    return rc::gen::exec(
        [pool = std::vector<DataType::Type>(pool.begin(), pool.end()), minFields, maxFields]()
        {
            const auto numFields = *rc::gen::inRange(minFields, maxFields + 1);
            std::vector<DataType> schema;
            schema.reserve(numFields);
            for (size_t i = 0; i < numFields; ++i)
            {
                const auto idx = *rc::gen::inRange(static_cast<size_t>(0), pool.size());
                const auto nullable = *rc::gen::arbitrary<bool>() ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
                schema.emplace_back(pool[idx], nullable);
            }
            return schema;
        });
}

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
rc::Gen<AnyVec> genAnyVec(std::vector<DataType> types)
{
    return rc::gen::exec(
        [types = std::move(types)]()
        {
            AnyVec result;
            result.reserve(types.size());
            for (const auto& dataType : types)
            {
                switch (dataType.type)
                {
                    case DataType::Type::UINT8:
                        result.push_back(genScalarAny<uint8_t>(dataType.nullable));
                        break;
                    case DataType::Type::UINT16:
                        result.push_back(genScalarAny<uint16_t>(dataType.nullable));
                        break;
                    case DataType::Type::UINT32:
                        result.push_back(genScalarAny<uint32_t>(dataType.nullable));
                        break;
                    case DataType::Type::UINT64:
                        result.push_back(genScalarAny<uint64_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT8:
                        result.push_back(genScalarAny<int8_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT16:
                        result.push_back(genScalarAny<int16_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT32:
                        result.push_back(genScalarAny<int32_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT64:
                        result.push_back(genScalarAny<int64_t>(dataType.nullable));
                        break;
                    case DataType::Type::FLOAT32:
                        result.push_back(genScalarAny<float>(dataType.nullable));
                        break;
                    case DataType::Type::FLOAT64:
                        result.push_back(genScalarAny<double>(dataType.nullable));
                        break;
                    case DataType::Type::VARSIZED: {
                        if (dataType.nullable)
                        {
                            const bool isNull = *rc::gen::arbitrary<bool>();
                            if (isNull)
                            {
                                result.emplace_back(std::optional<std::string>{});
                                break;
                            }
                            auto str = *rc::gen::container<std::string>(rc::gen::inRange<char>(PRINTABLE_ASCII_MIN, PRINTABLE_ASCII_MAX));
                            if (str.size() > MAX_VARSIZED_LEN)
                            {
                                str.resize(MAX_VARSIZED_LEN);
                            }
                            result.emplace_back(std::optional<std::string>{std::move(str)});
                            break;
                        }
                        auto str = *rc::gen::container<std::string>(rc::gen::inRange<char>(PRINTABLE_ASCII_MIN, PRINTABLE_ASCII_MAX));
                        if (str.size() > MAX_VARSIZED_LEN)
                        {
                            str.resize(MAX_VARSIZED_LEN);
                        }
                        result.emplace_back(std::move(str));
                        break;
                    }
                    case DataType::Type::BOOLEAN:
                    case DataType::Type::CHAR:
                    case DataType::Type::UNDEFINED:
                        throw TestException("Unsupported type for genAnyVec");
                }
            }
            return result;
        });
}

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

int compareAnyField(const std::any& lhs, const std::any& rhs, DataType type)
{
    switch (type.type)
    {
        case DataType::Type::UINT8:
            return compareTyped<uint8_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT16:
            return compareTyped<uint16_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT32:
            return compareTyped<uint32_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT64:
            return compareTyped<uint64_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT8:
            return compareTyped<int8_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT16:
            return compareTyped<int16_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT32:
            return compareTyped<int32_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT64:
            return compareTyped<int64_t>(lhs, rhs, type.nullable);
        case DataType::Type::FLOAT32:
            return compareTyped<float>(lhs, rhs, type.nullable);
        case DataType::Type::FLOAT64:
            return compareTyped<double>(lhs, rhs, type.nullable);
        case DataType::Type::VARSIZED:
            if (type.nullable)
            {
                const auto& left = std::any_cast<const std::optional<std::string>&>(lhs);
                const auto& right = std::any_cast<const std::optional<std::string>&>(rhs);
                if (left < right)
                {
                    return -1;
                }
                if (left > right)
                {
                    return 1;
                }
                return 0;
            }
            return std::any_cast<const std::string&>(lhs).compare(std::any_cast<const std::string&>(rhs));
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw TestException("Unsupported type for compareAnyField");
    }
    std::unreachable();
}

bool anyVecsEqual(const AnyVec& lhs, const AnyVec& rhs, const std::vector<DataType>& types)
{
    return std::ranges::all_of(
        std::views::zip(lhs, rhs, types),
        [](const auto& entry)
        {
            const auto& [left, right, dataType] = entry;
            return compareAnyField(left, right, dataType) == 0;
        });
}

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

void storeVarValToAnyVec(const nautilus::val<AnyVec*>& out, uint64_t pos, const VarVal& value, const DataType& dataType)
{
    switch (dataType.type)
    {
        case DataType::Type::UINT8:
            storeScalarToAnyVec<uint8_t>(out, pos, value.getRawValueAs<nautilus::val<uint8_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::UINT16:
            storeScalarToAnyVec<uint16_t>(out, pos, value.getRawValueAs<nautilus::val<uint16_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::UINT32:
            storeScalarToAnyVec<uint32_t>(out, pos, value.getRawValueAs<nautilus::val<uint32_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::UINT64:
            storeScalarToAnyVec<uint64_t>(out, pos, value.getRawValueAs<nautilus::val<uint64_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::INT8:
            storeScalarToAnyVec<int8_t>(out, pos, value.getRawValueAs<nautilus::val<int8_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::INT16:
            storeScalarToAnyVec<int16_t>(out, pos, value.getRawValueAs<nautilus::val<int16_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::INT32:
            storeScalarToAnyVec<int32_t>(out, pos, value.getRawValueAs<nautilus::val<int32_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::INT64:
            storeScalarToAnyVec<int64_t>(out, pos, value.getRawValueAs<nautilus::val<int64_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::FLOAT32:
            storeScalarToAnyVec<float>(out, pos, value.getRawValueAs<nautilus::val<float>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::FLOAT64:
            storeScalarToAnyVec<double>(out, pos, value.getRawValueAs<nautilus::val<double>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::VARSIZED: {
            const auto vsd = value.getRawValueAs<VariableSizedData>();
            if (dataType.nullable)
            {
                nautilus::invoke(
                    /// NOLINTNEXTLINE(readability-non-const-parameter): ptr matches the val<int8_t*> signature emitted by VariableSizedData::getContent().
                    +[](AnyVec* anyVec, uint64_t pos, int8_t* ptr, uint64_t len, bool null)
                    {
                        if (null)
                        {
                            (*anyVec)[pos] = std::any{std::optional<std::string>{}};
                            return;
                        }
                        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                        const auto* const cptr = reinterpret_cast<const char*>(ptr);
                        (*anyVec)[pos] = std::any{std::optional<std::string>{std::string(cptr, len)}};
                    },
                    out,
                    nautilus::val<uint64_t>{pos},
                    vsd.getContent(),
                    vsd.getSize(),
                    value.isNull());
                break;
            }
            nautilus::invoke(
                /// NOLINTNEXTLINE(readability-non-const-parameter): ptr matches the val<int8_t*> signature emitted by VariableSizedData::getContent().
                +[](AnyVec* anyVec, uint64_t pos, int8_t* ptr, uint64_t len)
                {
                    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                    const auto* const cptr = reinterpret_cast<const char*>(ptr);
                    (*anyVec)[pos] = std::any{std::string(cptr, len)};
                },
                out,
                nautilus::val<uint64_t>{pos},
                vsd.getContent(),
                vsd.getSize());
            break;
        }
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw TestException("Unsupported type for TestablePagedVector");
    }
}

nautilus::val<bool> checkIfNullInAnyVec(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, DataType::Type type)
{
    return nautilus::invoke(
        +[](AnyVec* anyVec, uint64_t pos, DataType::Type fieldType) -> bool
        {
            const auto& entry = (*anyVec)[pos];
            switch (fieldType)
            {
                case DataType::Type::UINT8:
                    return !std::any_cast<std::optional<uint8_t>>(entry).has_value();
                case DataType::Type::UINT16:
                    return !std::any_cast<std::optional<uint16_t>>(entry).has_value();
                case DataType::Type::UINT32:
                    return !std::any_cast<std::optional<uint32_t>>(entry).has_value();
                case DataType::Type::UINT64:
                    return !std::any_cast<std::optional<uint64_t>>(entry).has_value();
                case DataType::Type::INT8:
                    return !std::any_cast<std::optional<int8_t>>(entry).has_value();
                case DataType::Type::INT16:
                    return !std::any_cast<std::optional<int16_t>>(entry).has_value();
                case DataType::Type::INT32:
                    return !std::any_cast<std::optional<int32_t>>(entry).has_value();
                case DataType::Type::INT64:
                    return !std::any_cast<std::optional<int64_t>>(entry).has_value();
                case DataType::Type::FLOAT32:
                    return !std::any_cast<std::optional<float>>(entry).has_value();
                case DataType::Type::FLOAT64:
                    return !std::any_cast<std::optional<double>>(entry).has_value();
                case DataType::Type::VARSIZED:
                    return !std::any_cast<std::optional<std::string>>(entry).has_value();
                default:
                    std::unreachable();
            }
        },
        rec,
        nautilus::val<uint64_t>{fieldIdx},
        nautilus::val<DataType::Type>{type});
}

nautilus::val<AnyVec*> anyVecPushBack(const nautilus::val<std::vector<AnyVec>*>& vec, const nautilus::val<size_t>& numberOfFields)
{
    return nautilus::invoke(
        +[](std::vector<AnyVec>* outer, size_t numberOfFields)
        {
            outer->emplace_back(numberOfFields);
            return &outer->back();
        },
        vec,
        numberOfFields);
}

VarVal buildVarVal(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, DataType dataType)
{
    const nautilus::val<bool> isNull = dataType.nullable ? checkIfNullInAnyVec(rec, fieldIdx, dataType.type) : nautilus::val<bool>{false};
    switch (dataType.type)
    {
        case DataType::Type::UINT8:
            return {fetchScalarFromAnyVec<uint8_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::UINT16:
            return {fetchScalarFromAnyVec<uint16_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::UINT32:
            return {fetchScalarFromAnyVec<uint32_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::UINT64:
            return {fetchScalarFromAnyVec<uint64_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::INT8:
            return {fetchScalarFromAnyVec<int8_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::INT16:
            return {fetchScalarFromAnyVec<int16_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::INT32:
            return {fetchScalarFromAnyVec<int32_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::INT64:
            return {fetchScalarFromAnyVec<int64_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::FLOAT32:
            return {fetchScalarFromAnyVec<float>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::FLOAT64:
            return {fetchScalarFromAnyVec<double>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::VARSIZED: {
            auto ptr = nautilus::invoke(
                +[](AnyVec* anyVec, uint64_t pos, bool nullable) -> int8_t*
                {
                    const char* data = nullptr;
                    if (nullable)
                    {
                        const auto& opt = std::any_cast<const std::optional<std::string>&>((*anyVec)[pos]);
                        data = opt.has_value() ? opt->data() : nullptr;
                    }
                    else
                    {
                        data = std::any_cast<const std::string&>((*anyVec)[pos]).data();
                    }
                    /// reinterpret_cast: punning char* to int8_t* (same representation).
                    /// const_cast: the VariableSizedData/Nautilus API requires a mutable int8_t* even though
                    /// this code path only reads from the buffer; the reference behind `data` originates from
                    /// the std::any/std::string store and is not actually written through.
                    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast,cppcoreguidelines-pro-type-reinterpret-cast)
                    return const_cast<int8_t*>(reinterpret_cast<const int8_t*>(data));
                },
                rec,
                nautilus::val<uint64_t>{fieldIdx},
                nautilus::val<bool>{dataType.nullable});
            auto len = nautilus::invoke(
                +[](AnyVec* anyVec, uint64_t pos, bool nullable) -> uint64_t
                {
                    if (nullable)
                    {
                        const auto& opt = std::any_cast<const std::optional<std::string>&>((*anyVec)[pos]);
                        return opt.has_value() ? opt->size() : 0;
                    }
                    return std::any_cast<const std::string&>((*anyVec)[pos]).size();
                },
                rec,
                nautilus::val<uint64_t>{fieldIdx},
                nautilus::val<bool>{dataType.nullable});
            return {VariableSizedData(ptr, len), dataType.nullable, isNull};
        }
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            break;
    }
    throw TestException("Unsupported type for TestablePagedVector");
}

class TestableChainedHashMap
{
public:
    TestableChainedHashMap(
        const std::vector<DataType>& fieldTypes,
        AbstractBufferProvider& bufferManager,
        EngineMode mode,
        uint64_t numberOfBuckets,
        size_t numKeyFields,
        uint64_t numEntriesPerPage,
        const Nautilus::Interface::BloomFilterParams& bloomFilterParams = {})
        : dataTypes(fieldTypes), entriesPerPage(numEntriesPerPage), bufferManager(bufferManager), bloomFilterParams(bloomFilterParams)
    {
        const auto schema = createSchemaFromDataTypes(dataTypes);
        projections = schema.getFieldNames();
        auto offsets = computeFieldOffsets(fieldTypes, numKeyFields);
        fieldKeys = std::move(offsets.keys); /// NOLINT(cppcoreguidelines-prefer-member-initializer): projections must be set first
        fieldValues = std::move(offsets.values); /// NOLINT(cppcoreguidelines-prefer-member-initializer): projections must be set first

        engine = std::make_unique<nautilus::engine::NautilusEngine>(makeEngine(mode));

        /// NOLINT(cppcoreguidelines-narrowing-conversions)
        keyDataTypes = std::vector<DataType>(fieldTypes.begin(), fieldTypes.begin() + numKeyFields);
        /// NOLINT(cppcoreguidelines-narrowing-conversions)
        valueDataTypes = std::vector<DataType>(fieldTypes.begin() + numKeyFields, fieldTypes.end());

        const uint64_t keySize = createSchemaFromDataTypes(keyDataTypes).getSizeOfSchemaInBytes();
        const uint64_t valueSize = createSchemaFromDataTypes(valueDataTypes).getSizeOfSchemaInBytes();
        entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
        const uint64_t pageSize = entrySize * numEntriesPerPage;

        auto chainedHashMapBufferOpt = bufferManager.getUnpooledBuffer(ChainedHashMap::calculateBufferSizeFromBuckets(numberOfBuckets));
        if (not chainedHashMapBufferOpt.has_value())
        {
            throw BufferAllocationFailure(
                "No unpooled TupleBuffer of size {} available!", ChainedHashMap::calculateBufferSizeFromBuckets(numberOfBuckets));
        }
        chainedHashMapBuffer = chainedHashMapBufferOpt.value();
        std::ignore
            = ChainedHashMap::init(chainedHashMapBuffer, entrySize, numberOfBuckets, pageSize, bloomFilterParams.allocationByteCount());

        const auto numKeyProjections = keyDataTypes.size();

        /// NOLINT(cppcoreguidelines-narrowing-conversions)
        const auto keyProjections
            = std::vector<Record::RecordFieldIdentifier>(projections.begin(), projections.begin() + numKeyProjections);
        /// NOLINT(cppcoreguidelines-narrowing-conversions)
        const auto valueProjections
            = std::vector<Record::RecordFieldIdentifier>(projections.begin() + numKeyProjections, projections.end());

        /// NOLINTBEGIN(performance-unnecessary-value-param)
        insertFn.emplace(engine->registerFunction(std::function(
            [dataTypes = dataTypes,
             projections = projections,
             fieldKeys = fieldKeys,
             fieldValues = fieldValues,
             capturedEntriesPerPage = entriesPerPage,
             capturedEntrySize = entrySize,
             bloomFilter = createBloomFilterRef(),
             hashFn = MurMur3HashFunction{}](
                nautilus::val<TupleBuffer*> chainedHashMapBuffer, nautilus::val<AbstractBufferProvider*> bm, nautilus::val<AnyVec*> rec)
            {
                Record record;
                /// static_val ensures each field iteration gets a distinct trace tag.
                for (nautilus::static_val<uint64_t> i = 0; i < dataTypes.size(); ++i)
                {
                    record.write(projections[i], buildVarVal(rec, i, dataTypes[i]));
                }
                ChainedHashMapRef chmRef(
                    chainedHashMapBuffer,
                    fieldKeys,
                    fieldValues,
                    nautilus::val<uint64_t>(capturedEntriesPerPage),
                    nautilus::val<uint64_t>(capturedEntrySize),
                    bloomFilter);
                std::ignore = chmRef.findOrCreateEntry(
                    record,
                    hashFn,
                    [&](const nautilus::val<AbstractHashMapEntry*>& newEntry)
                    {
                        const auto chainedEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(newEntry);
                        const ChainedHashMapRef::ChainedEntryRef newEntryRef(chainedEntry, chainedHashMapBuffer, fieldKeys, fieldValues);
                        newEntryRef.copyValuesToEntry(record, bm);
                    },
                    bm);
            })));

        readAtFn.emplace(engine->registerFunction(std::function(
            [fieldKeys = fieldKeys, fieldValues = fieldValues, capturedEntriesPerPage = entriesPerPage, capturedEntrySize = entrySize](
                nautilus::val<TupleBuffer*> chainedHashMapBuffer, nautilus::val<uint64_t> index, nautilus::val<AnyVec*> out)
            {
                const nautilus::val<uint64_t> pageIndex = index / nautilus::val<uint64_t>(capturedEntriesPerPage);
                const nautilus::val<uint64_t> indexOnPage = index % nautilus::val<uint64_t>(capturedEntriesPerPage);
                const nautilus::val<uint64_t> offsetInPage = indexOnPage * nautilus::val<uint64_t>(capturedEntrySize);

                const auto entryPtr = nautilus::invoke(
                    +[](TupleBuffer* chmBuf, uint64_t pageIdx, uint64_t offset) -> ChainedHashMapEntry*
                    {
                        const ChainedHashMap chm = ChainedHashMap::load(*chmBuf);
                        auto page = chm.getPage(pageIdx);
                        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                        return reinterpret_cast<ChainedHashMapEntry*>(page.getAvailableMemoryArea().subspan(offset).data());
                    },
                    chainedHashMapBuffer,
                    pageIndex,
                    offsetInPage);

                const ChainedHashMapRef::ChainedEntryRef entryRef(entryPtr, chainedHashMapBuffer, fieldKeys, fieldValues);

                const auto keyRecord = entryRef.getKey();
                const auto valueRecord = entryRef.getValue();

                for (nautilus::static_val<uint64_t> i = 0; i < fieldKeys.size(); ++i)
                {
                    const auto& fld = fieldKeys[i];
                    storeVarValToAnyVec(out, static_cast<uint64_t>(i), keyRecord.read(fld.fieldIdentifier), fld.type);
                }
                for (nautilus::static_val<uint64_t> i = 0; i < fieldValues.size(); ++i)
                {
                    const auto& fld = fieldValues[i];
                    storeVarValToAnyVec(out, fieldKeys.size() + static_cast<uint64_t>(i), valueRecord.read(fld.fieldIdentifier), fld.type);
                }
            })));

        readAllFn.emplace(engine->registerFunction(std::function(
            [fieldKeys = fieldKeys,
             fieldValues = fieldValues,
             capturedEntriesPerPage = entriesPerPage,
             capturedEntrySize = entrySize,
             bloomFilter = createBloomFilterRef()](
                nautilus::val<TupleBuffer*> chainedHashMapBuffer, nautilus::val<std::vector<AnyVec>*> outVector)
            {
                /// begin() calls getPage(0) via invoke which fails on an empty CHM, so guard first.
                const auto numTuples = nautilus::invoke(
                    +[](TupleBuffer* buf) { return ChainedHashMap::load(*buf).getTotalNumberOfRecords(); }, chainedHashMapBuffer);
                if (numTuples == nautilus::val<uint64_t>(0))
                {
                    return;
                }

                const ChainedHashMapRef chmRef(
                    chainedHashMapBuffer,
                    fieldKeys,
                    fieldValues,
                    nautilus::val<uint64_t>(capturedEntriesPerPage),
                    nautilus::val<uint64_t>(capturedEntrySize),
                    bloomFilter);

                for (const auto entry : chmRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRef(entry, chainedHashMapBuffer, fieldKeys, fieldValues);

                    auto out = anyVecPushBack(outVector, nautilus::val<size_t>(fieldKeys.size() + fieldValues.size()));

                    const auto keyRecord = entryRef.getKey();
                    const auto valueRecord = entryRef.getValue();

                    for (nautilus::static_val<uint64_t> i = 0; i < fieldKeys.size(); ++i)
                    {
                        const auto& fld = fieldKeys[i];
                        storeVarValToAnyVec(out, static_cast<uint64_t>(i), keyRecord.read(fld.fieldIdentifier), fld.type);
                    }
                    for (nautilus::static_val<uint64_t> i = 0; i < fieldValues.size(); ++i)
                    {
                        const auto& fld = fieldValues[i];
                        storeVarValToAnyVec(
                            out, fieldKeys.size() + static_cast<uint64_t>(i), valueRecord.read(fld.fieldIdentifier), fld.type);
                    }
                }
            })));
        /// NOLINTEND(performance-unnecessary-value-param)
    }

    ~TestableChainedHashMap() = default;
    TestableChainedHashMap(const TestableChainedHashMap&) = delete;
    TestableChainedHashMap& operator=(const TestableChainedHashMap&) = delete;
    TestableChainedHashMap(TestableChainedHashMap&&) = default;
    TestableChainedHashMap& operator=(TestableChainedHashMap&&) = delete;

    /// const_cast: insert's signature requires AnyVec* even though the trace lambda only reads from it.
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast, bugprone-unchecked-optional-access)
    void insert(const AnyVec& record) { (*insertFn)(&chainedHashMapBuffer, &bufferManager, const_cast<AnyVec*>(&record)); }

    AnyVec readAt(uint64_t index)
    {
        AnyVec out(dataTypes.size());
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        (*readAtFn)(&chainedHashMapBuffer, index, &out);
        return out;
    }

    std::vector<AnyVec> toVector()
    {
        const auto numEntries = ChainedHashMap::load(chainedHashMapBuffer).getTotalNumberOfRecords();
        std::vector<AnyVec> out;
        out.reserve(numEntries);
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        (*readAllFn)(&chainedHashMapBuffer, &out);
        return out;
    }

    [[nodiscard]] uint64_t size() const { return ChainedHashMap::load(chainedHashMapBuffer).getTotalNumberOfRecords(); }

    ChainedHashMap raw() { return ChainedHashMap::load(chainedHashMapBuffer); }

    [[nodiscard]] size_t numKeyFields() const { return keyDataTypes.size(); }

    [[nodiscard]] const std::vector<DataType>& getKeyDataTypes() const { return keyDataTypes; }

private:
    std::vector<DataType> dataTypes;
    std::vector<DataType> keyDataTypes;
    std::vector<DataType> valueDataTypes;
    std::vector<FieldOffsets> fieldKeys;
    std::vector<FieldOffsets> fieldValues;
    uint64_t entrySize{0};
    uint64_t entriesPerPage{0};
    TupleBuffer chainedHashMapBuffer;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    AbstractBufferProvider& bufferManager;
    /// Sizing for the optional in-map BloomFilter. Defaults to disabled (hashCount == 0 -> NoOp), so the
    /// CHM behaves exactly as before unless a test opts in via the constructor.
    Nautilus::Interface::BloomFilterParams bloomFilterParams;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    std::optional<nautilus::engine::CallableFunction<void, TupleBuffer*, AbstractBufferProvider*, AnyVec*>> insertFn;
    std::optional<nautilus::engine::CallableFunction<void, TupleBuffer*, uint64_t, AnyVec*>> readAtFn;
    std::optional<nautilus::engine::CallableFunction<void, TupleBuffer*, std::vector<AnyVec>*>> readAllFn;

    /// Builds the ChainedHashMapRef's BloomFilterRef from bloomFilterParams. The CHM reuses each entry's
    /// stored 64-bit hash, so no HashFunction is needed. hashCount == 0 yields the disabled NoOp variant.
    [[nodiscard]] Nautilus::Interface::BloomFilterRef createBloomFilterRef() const
    {
        return Nautilus::Interface::BloomFilterRef::create(
            bloomFilterParams.hashCount > 0, bloomFilterParams.bitCount, bloomFilterParams.hashCount, nullptr);
    }

    struct FieldOffsets
    {
        decltype(fieldKeys) keys;
        decltype(fieldValues) values;
    };

    FieldOffsets computeFieldOffsets(const std::vector<DataType>& fieldTypes, size_t numKeyFields)
    {
        /// NOLINT(cppcoreguidelines-narrowing-conversions)
        const auto splitPoint = std::next(projections.begin(), numKeyFields);
        const auto keyProjections = std::vector<Record::RecordFieldIdentifier>(projections.begin(), splitPoint);
        const auto valueProjections = std::vector<Record::RecordFieldIdentifier>(splitPoint, projections.end());
        auto [fk, fv]
            = ChainedEntryMemoryProvider::createFieldOffsets(createSchemaFromDataTypes(fieldTypes), keyProjections, valueProjections);
        return {std::move(fk), std::move(fv)};
    }
};

/// Reads the CHM at a random sample of indices and verifies each returned entry's key exists in the
/// reference and its stored value matches the first-seen value for that key.
void verifyRandomAccess(
    TestableChainedHashMap& chainedHashMap, const std::vector<AnyVec>& reference, const std::vector<DataType>& fieldTypes)
{
    if (reference.empty())
    {
        return;
    }
    const size_t numKeys = chainedHashMap.numKeyFields();
    const auto& keyTypes = chainedHashMap.getKeyDataTypes();
    const auto allEntries = chainedHashMap.toVector();
    if (allEntries.empty())
    {
        return;
    }
    const auto indices = *rc::gen::container<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(0, allEntries.size()));
    for (const auto idx : indices)
    {
        const auto& actual = allEntries[idx];
        const auto it = std::ranges::find_if(
            reference,
            [&](const AnyVec& expected)
            {
                for (size_t k = 0; k < numKeys; ++k)
                {
                    if (compareAnyField(actual[k], expected[k], keyTypes[k]) != 0)
                    {
                        return false;
                    }
                }
                return true;
            });
        RC_ASSERT(it != reference.end());
        RC_ASSERT(anyVecsEqual(actual, *it, fieldTypes));
    }
}

} /// anonymous namespace

/// Generates random *enabled* BloomFilter sizing. The lower bit-count end is deliberately tiny (saturated
/// -> high false-positive rate) and the upper end sparse, stressing that correctness holds at any FP rate:
/// a BloomFilter must never yield a false negative, so a CHM with the filter enabled must dedup and iterate
/// to exactly the same result as the disabled run for any sizing.
static Nautilus::Interface::BloomFilterParams genBloomFilterParams()
{
    const auto bitCount = *rc::gen::inRange<uint64_t>(64, 1U << 20U);
    const auto hashCount = *rc::gen::inRange<uint64_t>(1, 11);
    return {.bitCount = bitCount, .hashCount = hashCount};
}

/// Builds a reference vector tracking only the first-seen record per unique key in insertion order,
/// mirroring CHM's first-write-wins deduplication and sequential page-slot allocation order.
static std::vector<AnyVec>
buildUniqueKeyReference(TestableChainedHashMap& chainedHashMap, const std::vector<DataType>& fieldTypes, uint64_t numberOfItems)
{
    std::vector<AnyVec> uniqueReference;
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes);
        const size_t numKeys = chainedHashMap.numKeyFields();
        const auto& keyTypes = chainedHashMap.getKeyDataTypes();
        const bool keyAlreadySeen = std::ranges::any_of(
            uniqueReference,
            [&](const AnyVec& existing)
            {
                for (size_t k = 0; k < numKeys; ++k)
                {
                    if (compareAnyField(record[k], existing[k], keyTypes[k]) != 0)
                    {
                        return false;
                    }
                }
                return true;
            });
        if (!keyAlreadySeen)
        {
            uniqueReference.push_back(record);
        }
        chainedHashMap.insert(record);
    }
    return uniqueReference;
}

/// Verify CHM deduplicates correctly and positional readAt(i) returns the i-th first-seen unique-key entry.
/// When enableBloomFilter is set, the in-map BloomFilter is enabled at random sizing; a pass proves the
/// results are identical to the disabled run, i.e. the filter never produced a false negative.
static void insertAndIterateKeysProperty(EngineMode mode, bool enableBloomFilter = false)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numKeyFields = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);
    const auto bloomFilterParams = enableBloomFilter ? genBloomFilterParams() : Nautilus::Interface::BloomFilterParams{};

    NES_INFO(
        "Property insertAndIterateKeys: fields={}, N={}, bufferSize={}, numKeyFields={}, numBuckets={}, entriesPerPage={}, "
        "bloomBits={}, bloomHashes={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        numKeyFields,
        numberOfBuckets,
        numEntriesPerPage,
        bloomFilterParams.bitCount,
        bloomFilterParams.hashCount,
        fmt::join(fieldTypes, ", "));

    auto bufferManager = DirtyBufferProvider::create(bufferSize, pooledBufferCountFor(bufferSize));
    TestableChainedHashMap chainedHashMap(
        fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage, bloomFilterParams);
    const auto uniqueReference = buildUniqueKeyReference(chainedHashMap, fieldTypes, numberOfItems);

    NES_INFO("insertAndIterateKeys: CHM has {} entries, {} unique keys", chainedHashMap.size(), uniqueReference.size());
    RC_ASSERT(chainedHashMap.size() == uniqueReference.size());
    verifyRandomAccess(chainedHashMap, uniqueReference, fieldTypes);
}

/// Verify CHM iteration via toVector() returns every stored entry with the correct key+value pair.
/// When enableBloomFilter is set, the in-map BloomFilter is enabled at random sizing; a pass proves the
/// stored entries are identical to the disabled run, i.e. the filter never produced a false negative.
static void insertAndIterateRecordsProperty(EngineMode mode, bool enableBloomFilter = false)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numKeyFields = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);
    const auto bloomFilterParams = enableBloomFilter ? genBloomFilterParams() : Nautilus::Interface::BloomFilterParams{};

    NES_INFO(
        "Property insertAndIterateRecords: fields={}, N={}, bufferSize={}, numKeyFields={}, numBuckets={}, entriesPerPage={}, "
        "bloomBits={}, bloomHashes={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        numKeyFields,
        numberOfBuckets,
        numEntriesPerPage,
        bloomFilterParams.bitCount,
        bloomFilterParams.hashCount,
        fmt::join(fieldTypes, ", "));

    auto bufferManager = DirtyBufferProvider::create(bufferSize, pooledBufferCountFor(bufferSize));
    TestableChainedHashMap chainedHashMap(
        fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage, bloomFilterParams);
    const auto uniqueReference = buildUniqueKeyReference(chainedHashMap, fieldTypes, numberOfItems);

    NES_INFO("insertAndIterateRecords: CHM has {} entries, {} unique keys", chainedHashMap.size(), uniqueReference.size());
    RC_ASSERT(chainedHashMap.size() == uniqueReference.size());

    /// Iterate all entries via the CHM range-for (page-sequential order) and verify each stored
    /// key+value pair matches the first-seen record for that key.
    auto actual = chainedHashMap.toVector();
    RC_ASSERT(actual.size() == uniqueReference.size());
    for (const auto& expected : uniqueReference)
    {
        const bool found = std::ranges::any_of(actual, [&](const AnyVec& anyVec) { return anyVecsEqual(anyVec, expected, fieldTypes); });
        RC_ASSERT(found);
    }
}

/// One RC_GTEST_PROP per (property, backend) combination so that a failure on one backend doesn't mask the other and
/// rapidcheck's shrinking chases each backend's failing input independently.
RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndIterateKeysCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateKeysProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndIterateKeysInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateKeysProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndIterateRecordsCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateRecordsProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndIterateRecordsInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateRecordsProperty(EngineMode::Interpreter);
}

/// Same properties, but with the in-map BloomFilter enabled at random sizing. findOrCreateEntry consults
/// the filter in findChain; a false negative would re-insert an existing key and break dedup, so a pass
/// proves the results are identical to the disabled runs above for the randomly-chosen filter sizing.
RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndIterateKeysWithBloomFilterCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateKeysProperty(EngineMode::Compiler, true);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndIterateKeysWithBloomFilterInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateKeysProperty(EngineMode::Interpreter, true);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndIterateRecordsWithBloomFilterCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateRecordsProperty(EngineMode::Compiler, true);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndIterateRecordsWithBloomFilterInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateRecordsProperty(EngineMode::Interpreter, true);
}

}
