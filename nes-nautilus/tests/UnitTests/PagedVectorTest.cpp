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
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
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

/// NOLINTBEGIN(misc-include-cleaner, bugprone-unchecked-optional-access)
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
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.compilationStrategy", std::string("legacy"));
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
constexpr std::array<uint64_t, 5> BUFFER_SIZE_POOL = {64, 128, 512, 4096, 2ULL * 1024 * 1024};

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
Schema<QualifiedUnboundField, Ordered> createSchemaFromDataTypes(const std::vector<DataType>& dataTypes)
{
    return views::enumerate(dataTypes)
        | std::views::transform(
               [](const auto& pair)
               {
                   const auto& [typeIdx, dataType] = pair;
                   const auto name = Record::RecordFieldIdentifier(Identifier::parse("field" + std::to_string(typeIdx)));
                   return QualifiedUnboundField{name, dataType};
               })
        | std::ranges::to<Schema<QualifiedUnboundField, Ordered>>();
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

uint64_t estimateSchemaSize(const std::vector<DataType>& types)
{
    return createSchemaFromDataTypes(types).getSizeInBytes();
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
            outer->emplace_back(numberOfFields, 0);
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

/// Mirrors std::vector<AnyVec> but internally operates on a real PagedVector.
/// insert and readAt are Nautilus-compiled and invoked via function-pointer dispatch.
class TestablePagedVector
{
public:
    TestablePagedVector(const std::vector<DataType>& fieldTypes, AbstractBufferProvider& bufferManager, EngineMode mode)
        : dataTypes(fieldTypes), bufferManager(bufferManager)
    {
        const auto schema = createSchemaFromDataTypes(dataTypes);
        projections = schema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
            | std::ranges::to<std::vector>();
        auto layout = std::make_shared<DefaultPagedVectorTupleLayout>(schema);

        nautilus::engine::Options options;
        options.setOption("engine.Compilation", true);
        options.setOption("mlir.enableMultithreading", false);
        engine = std::make_unique<nautilus::engine::NautilusEngine>(makeEngine(mode));

        pagedVector = bufferManager.getUnpooledBuffer(PagedVector::getMainBufferSize()).value();
        PagedVector::init(pagedVector, bufferManager.getBufferSize(), layout->getSchema().getSizeInBytes());

        /// NOLINTBEGIN(performance-unnecessary-value-param)
        pushbackFn.emplace(engine->registerFunction(std::function(
            [layout, dataTypes = dataTypes, projections = projections](
                nautilus::val<TupleBuffer*> pagedVector, nautilus::val<AbstractBufferProvider*> bm, nautilus::val<AnyVec*> rec)
            {
                Record record;
                /// static_val ensures each field iteration gets a distinct trace tag.
                for (nautilus::static_val<uint64_t> i = 0; i < dataTypes.size(); ++i)
                {
                    record.write(projections[i], buildVarVal(rec, i, dataTypes[i]));
                }
                PagedVectorRef pvRef(BorrowedNautilusBuffer::from(pagedVector), layout);
                pvRef.pushBack(record, bm);
            })));

        readAtFn.emplace(engine->registerFunction(std::function(
            [layout, dataTypes = dataTypes, projections = projections](
                nautilus::val<TupleBuffer*> pagedVector, nautilus::val<uint64_t> index, nautilus::val<AnyVec*> out)
            {
                const PagedVectorRef pvRef(BorrowedNautilusBuffer::from(pagedVector), layout);
                auto record = pvRef.at(index);
                for (nautilus::static_val<uint64_t> i = 0; i < dataTypes.size(); ++i)
                {
                    storeVarValToAnyVec(out, i, record.read(projections[i]), dataTypes[i]);
                }
            })));

        readAll.emplace(engine->registerFunction(std::function(
            [layout, dataTypes = dataTypes, projections = projections](
                nautilus::val<TupleBuffer*> pagedVector, nautilus::val<std::vector<AnyVec>*> outVector)
            {
                const PagedVectorRef pvRef(BorrowedNautilusBuffer::from(pagedVector), layout);
                for (const auto& record : pvRef)
                {
                    auto out = anyVecPushBack(outVector, nautilus::val<size_t>(std::ranges::size(layout->getSchema())));
                    for (nautilus::static_val<uint64_t> i = 0; i < dataTypes.size(); ++i)
                    {
                        storeVarValToAnyVec(out, i, record.read(projections[i]), dataTypes[i]);
                    }
                }
            })));
        /// NOLINTEND(performance-unnecessary-value-param)
    }

    ~TestablePagedVector() = default;
    TestablePagedVector(const TestablePagedVector&) = delete;
    TestablePagedVector& operator=(const TestablePagedVector&) = delete;
    TestablePagedVector(TestablePagedVector&&) = default;
    TestablePagedVector& operator=(TestablePagedVector&&) = delete;

    /// const_cast: pushbackFn's signature requires AnyVec* even though the trace lambda only reads from it.
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    void pushBack(const AnyVec& record) { (*pushbackFn)(&pagedVector, &bufferManager, const_cast<AnyVec*>(&record)); }

    AnyVec readAt(uint64_t index)
    {
        AnyVec out(dataTypes.size());
        (*readAtFn)(&pagedVector, index, &out);
        return out;
    }

    std::vector<AnyVec> toVector()
    {
        const auto numEntries = PagedVector::load(pagedVector).getTotalNumberOfRecords();
        std::vector<AnyVec> out;
        out.reserve(numEntries);
        (*readAll)(&pagedVector, &out);
        return out;
    }

    void concatMove(TestablePagedVector& other)
    {
        auto otherPagedVector = PagedVector::load(other.pagedVector);
        PagedVector::load(pagedVector).movePagesFrom(otherPagedVector);
    }

    void concatCopy(TestablePagedVector& other)
    {
        auto otherPagedVector = PagedVector::load(other.pagedVector);
        PagedVector::load(pagedVector).copyPagesFrom(bufferManager, otherPagedVector);
    }

    [[nodiscard]] uint64_t size() const { return PagedVector::load(pagedVector).getTotalNumberOfRecords(); }

    PagedVector raw() { return PagedVector::load(pagedVector); }

private:
    std::vector<DataType> dataTypes;
    TupleBuffer pagedVector;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    AbstractBufferProvider& bufferManager;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, AbstractBufferProvider*, AnyVec*)>> pushbackFn;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, uint64_t, AnyVec*)>> readAtFn;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, std::vector<AnyVec>*)>> readAll;
};

/// Reads pagedVector.at(idx) for a rapidcheck-drawn set of indices and asserts each record equals the reference.
void verifyRandomAccess(TestablePagedVector& pagedVector, const std::vector<AnyVec>& reference, const std::vector<DataType>& fieldTypes)
{
    if (reference.empty())
    {
        return;
    }
    const auto indices = *rc::gen::container<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(0, reference.size()));
    for (const auto idx : indices)
    {
        const auto actual = pagedVector.readAt(idx);
        RC_ASSERT(anyVecsEqual(actual, reference[idx], fieldTypes));
    }
}

/// Insert N items into a PagedVector, then iterate and compare against reference.
void insertAndIterateProperty(EngineMode mode)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_PROPERTY);

    NES_INFO(
        "Property insertAndIterate: fields={}, N={}, bufferSize={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        fmt::join(fieldTypes, ", "));

    auto bufferManager = DirtyBufferProvider::create(bufferSize, pooledBufferCountFor(bufferSize));
    TestablePagedVector pagedVector(fieldTypes, *bufferManager, mode);

    std::vector<AnyVec> reference;
    reference.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes);
        reference.push_back(record);
        pagedVector.pushBack(record);
    }

    NES_INFO("insertAndIterate: PagedVector has {} entries", pagedVector.size());
    RC_ASSERT(pagedVector.size() == reference.size());

    verifyRandomAccess(pagedVector, reference, fieldTypes);

    auto actual = pagedVector.toVector();
    RC_ASSERT(actual.size() == reference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(anyVecsEqual(actual[i], reference[i], fieldTypes));
    }
}

/// Insert N items into a PagedVector, then read each by index and compare against reference.
void insertAndReadByIndexProperty(EngineMode mode)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_PROPERTY);

    NES_INFO("Property insertAndReadByIndex: fields={}, N={}, bufferSize={}", fieldTypes.size(), numberOfItems, bufferSize);

    auto bufferManager = DirtyBufferProvider::create(bufferSize, pooledBufferCountFor(bufferSize));
    TestablePagedVector pagedVector(fieldTypes, *bufferManager, mode);

    std::vector<AnyVec> reference;
    reference.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes);
        reference.push_back(record);
        pagedVector.pushBack(record);
    }

    RC_ASSERT(pagedVector.size() == reference.size());

    verifyRandomAccess(pagedVector, reference, fieldTypes);

    for (uint64_t idx = 0; idx < reference.size(); ++idx)
    {
        auto actual = pagedVector.readAt(idx);
        RC_ASSERT(anyVecsEqual(actual, reference[idx], fieldTypes));
    }
}

/// Create K PagedVectors, insert items, concatMove via movePagesFrom, verify against concatenated reference.
void concatMoveProperty(EngineMode mode)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);

    const auto numVectors = *rc::gen::inRange<uint64_t>(1, MAX_CONCAT_VECTORS);

    NES_INFO("Property concatMove: fields={}, numVectors={}, bufferSize={}", fieldTypes.size(), numVectors, bufferSize);

    auto bufferManager = DirtyBufferProvider::create(bufferSize, pooledBufferCountFor(bufferSize));

    std::vector<TestablePagedVector> pagedVectors;
    pagedVectors.reserve(numVectors);
    std::vector<AnyVec> fullReference;
    for (uint64_t vecIdx = 0; vecIdx < numVectors; ++vecIdx)
    {
        const auto itemCount = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_CONCAT_VECTOR);
        pagedVectors.emplace_back(fieldTypes, *bufferManager, mode);
        for (uint64_t i = 0; i < itemCount; ++i)
        {
            auto record = *genAnyVec(fieldTypes);
            fullReference.push_back(record);
            pagedVectors.back().pushBack(record);
        }
        NES_INFO("concatMove: vector {} has {} entries", vecIdx, pagedVectors.back().size());
    }

    for (uint64_t vecIdx = 1; vecIdx < numVectors; ++vecIdx)
    {
        pagedVectors[0].concatMove(pagedVectors[vecIdx]);
        RC_ASSERT(pagedVectors[vecIdx].raw().getStatus() == PagedVector::INVALID_PV);
    }

    NES_INFO("concatMove: merged vector has {} entries, reference has {} entries", pagedVectors[0].size(), fullReference.size());
    RC_ASSERT(pagedVectors[0].size() == fullReference.size());

    verifyRandomAccess(pagedVectors[0], fullReference, fieldTypes);

    auto actual = pagedVectors[0].toVector();
    RC_ASSERT(actual.size() == fullReference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(anyVecsEqual(actual[i], fullReference[i], fieldTypes));
    }
}

void concatCopyProperty(EngineMode mode)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);
    const auto numVectors = *rc::gen::inRange<uint64_t>(1, MAX_CONCAT_VECTORS);
    NES_INFO("Property concatCopy: fields={}, numVectors={}, bufferSize={}", fieldTypes.size(), numVectors, bufferSize);
    auto bufferManager = DirtyBufferProvider::create(bufferSize, pooledBufferCountFor(bufferSize));
    std::vector<TestablePagedVector> pagedVectors;
    pagedVectors.reserve(numVectors);
    std::vector<AnyVec> fullReference;

    for (uint64_t vecIdx = 0; vecIdx < numVectors; ++vecIdx)
    {
        const auto itemCount = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_CONCAT_VECTOR);
        pagedVectors.emplace_back(fieldTypes, *bufferManager, mode);
        for (uint64_t i = 0; i < itemCount; ++i)
        {
            auto record = *genAnyVec(fieldTypes);
            fullReference.push_back(record);
            pagedVectors.back().pushBack(record);
        }
        NES_INFO("concatCopy: vector {} has {} entries", vecIdx, pagedVectors.back().size());
    }

    /// Snapshot each source vector's contents before any copy so we can verify that copyPagesFrom does not mutate or share memory with the sources.
    std::vector<std::vector<AnyVec>> sourceSnapshots;
    sourceSnapshots.reserve(numVectors);
    for (uint64_t vecIdx = 0; vecIdx < numVectors; ++vecIdx)
    {
        sourceSnapshots.push_back(pagedVectors[vecIdx].toVector());
    }

    for (uint64_t vecIdx = 1; vecIdx < numVectors; ++vecIdx)
    {
        auto numPagesPrevOther = pagedVectors[vecIdx].raw().getNumberOfPages();
        auto numTuplesPrevOther = pagedVectors[vecIdx].size();
        auto numPagesPrevFirst = pagedVectors[0].raw().getNumberOfPages();
        auto numTuplesPrevFirst = pagedVectors[0].size();
        pagedVectors[0].concatCopy(pagedVectors[vecIdx]);
        RC_ASSERT(pagedVectors[vecIdx].raw().getStatus() == PagedVector::VALID_PV);
        RC_ASSERT(pagedVectors[vecIdx].raw().getNumberOfPages() == numPagesPrevOther);
        RC_ASSERT(pagedVectors[vecIdx].size() == numTuplesPrevOther);
        RC_ASSERT(pagedVectors[0].raw().getNumberOfPages() == numPagesPrevFirst + numPagesPrevOther);
        RC_ASSERT(pagedVectors[0].size() == numTuplesPrevFirst + numTuplesPrevOther);

        /// Verify the source's records are intact after being copied into the destination, catching any accidental memory sharing between source and destination pages.
        auto sourceActual = pagedVectors[vecIdx].toVector();
        RC_ASSERT(sourceActual.size() == sourceSnapshots[vecIdx].size());
        for (size_t i = 0; i < sourceActual.size(); ++i)
        {
            RC_ASSERT(anyVecsEqual(sourceActual[i], sourceSnapshots[vecIdx][i], fieldTypes));
        }
    }

    NES_INFO("concatCopy: merged vector has {} entries, reference has {} entries", pagedVectors[0].size(), fullReference.size());
    RC_ASSERT(pagedVectors[0].size() == fullReference.size());

    verifyRandomAccess(pagedVectors[0], fullReference, fieldTypes);

    auto actual = pagedVectors[0].toVector();
    RC_ASSERT(actual.size() == fullReference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(anyVecsEqual(actual[i], fullReference[i], fieldTypes));
    }
}


} /// anonymous namespace

/// One RC_GTEST_PROP per (property, backend) combination so that a failure on one backend doesn't mask the other and
/// rapidcheck's shrinking chases each backend's failing input independently.
RC_GTEST_PROP(PagedVectorPropertyTest, insertAndIterateCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndIterateInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndReadByIndexCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndReadByIndexProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndReadByIndexInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndReadByIndexProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatMovePagedVectorCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatMoveProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatMovePagedVectorInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatMoveProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatCopyPagedVectorCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatCopyProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatCopyPagedVectorInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatCopyProperty(EngineMode::Interpreter);
}

}

/// NOLINTEND(misc-include-cleaner, bugprone-unchecked-optional-access)
