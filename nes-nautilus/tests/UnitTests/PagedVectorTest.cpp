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
#include <cmath>
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
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed via macros expanded from rapidcheck/gtest.h
#include <nautilus/Engine.hpp>
#include <nautilus/function.hpp>
#include <nautilus/select.hpp>
#include <Arena.hpp>
#include <BaseUnitTest.hpp>
#include <CompilationContext.hpp>
#include <DataStructureTestUtils.hpp>
#include <ErrorHandling.hpp>
#include <TestablePagedVector.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

#include <Util/Ranges.hpp>

/// Umbrella header: rapidcheck spreads rc::gen and the DefaultArbitrary specialisations over impl headers
/// (gen/*.hpp) that cannot be included directly, so the umbrella is the only supported entry point.
#include <rapidcheck.h>
#include <fmt/ranges.h>
#include <rapidcheck/gtest.h>

namespace NES
{
namespace
{
/// Marker pattern written into freshly handed-out buffers so tests notice if the PagedVector
/// reads uninitialised memory: a dirty page surfaces as 0xDEADBEEF instead of zero.
constexpr uint32_t DIRTY_FILL_PATTERN = 0xDEADBEEF;
constexpr NES::BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;

struct DirtyBufferProvider : AbstractBufferProvider
{
    explicit DirtyBufferProvider(std::shared_ptr<BufferManager> bm) : bm(std::move(bm)) { }

    static std::shared_ptr<DirtyBufferProvider> create(size_t bufferSize, size_t numberOfBuffers)
    {
        return std::make_shared<DirtyBufferProvider>(BufferManager::create(
            10 * numberOfBuffers * bufferSize,
            UNPOOLED_MEMORY_FRACTION,
            BUFFER_ALIGNMENT,
            static_cast<uint32_t>(bufferSize),
            std::make_shared<NesDefaultMemoryAllocator>()));
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

/// Wraps a real BufferManager but refuses (returns nullopt from) getUnpooledBuffer for any request larger than a
/// pooled buffer -- i.e. exactly the oversized-varsized allocation path in makeVarSizedAllocFunction -- while
/// leaving small unpooled requests (like PagedVector's own header buffer) and all pooled-buffer paths untouched.
/// This lets a plain unit test deterministically hit the "no unpooled buffer available for oversized varsized
/// value" throw without needing to actually exhaust memory.
struct NoOversizedUnpooledBufferProvider : AbstractBufferProvider
{
    explicit NoOversizedUnpooledBufferProvider(std::shared_ptr<BufferManager> bm) : bm(std::move(bm)) { }

    [[nodiscard]] BufferManagerType getBufferManagerType() const override { return bm->getBufferManagerType(); }

    [[nodiscard]] size_t getBufferSize() const override { return bm->getBufferSize(); }

    [[nodiscard]] size_t getNumOfPooledBuffers() const override { return bm->getNumOfPooledBuffers(); }

    [[nodiscard]] size_t getNumOfUnpooledBuffers() const override { return bm->getNumOfUnpooledBuffers(); }

    TupleBuffer getBufferBlocking() override { return bm->getBufferBlocking(); }

    std::optional<TupleBuffer> getBufferNoBlocking() override { return bm->getBufferNoBlocking(); }

    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeoutMs) override
    {
        return bm->getBufferWithTimeout(timeoutMs);
    }

    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) override
    {
        if (bufferSize > bm->getBufferSize())
        {
            return std::nullopt;
        }
        return bm->getUnpooledBuffer(bufferSize);
    }

    std::shared_ptr<BufferManager> bm;
};

/// Buffer-size pool drawn from per property to exercise both extremes:
/// - tiny buffers (64-512 B) force long page chains, which surfaces correctness issues in the
///   getPageIndex binary search and the last-page cumulative-sum special case;
/// - the large 2 MiB buffer keeps everything on a single page and exercises the no-paging path.
/// Schemas whose tuple size doesn't fit must be discarded via RC_PRE.
constexpr std::array<uint64_t, 5> BUFFER_SIZE_POOL = {64, 128, 512, 4096, 2ULL * 1024 * 1024};

/// Per-vector item-count range and max number of paged vectors used by the concat properties.
constexpr uint64_t MAX_ITEMS_PER_CONCAT_VECTOR = 201;
constexpr uint64_t MAX_CONCAT_VECTORS = 5;

/// Scalar-only subset of TestUtils::ALL_VALUE_TYPES (no VARSIZED), used by the dedicated oversized-varsized property to
/// build small schemas with a guaranteed, separately-controlled VARSIZED field appended.
constexpr std::array SCALAR_VALUE_TYPES = {
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
};

constexpr std::array FIXED_WIDTH_VALUE_TYPES = {
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
    DataType::Type::BOOLEAN,
    DataType::Type::CHAR,
};

/// Varsized payload lengths are occasionally drawn relative to the property's chosen bufferSize (not just
/// [0, TestUtils::MAX_VARSIZED_LEN]) so that some generated values don't fit in a single pooled buffer.
/// TestUtils::MAX_VARSIZED_LEN historically equalled the smallest BUFFER_SIZE_POOL entry (64), which meant a
/// generated payload could never exceed a pooled buffer's capacity -- silently preventing the
/// AbstractBufferProvider::getUnpooledBuffer fallback path in makeVarSizedAllocFunction from ever being exercised
/// with an oversized value.
///
/// This must be gated by an absolute per-run budget rather than a flat per-field probability: a wide schema times a
/// large item count means a single property run can generate thousands of varsized fields, so even a low per-field
/// percentage of "needs a dedicated pooled buffer" draws can exhaust DirtyBufferProvider's pooled buffer pool --
/// which is clamped as low as 16-32 buffers for the largest BUFFER_SIZE_POOL entry (2 MiB) under
/// TestUtils::pooledBufferCountFor's memory cap. Capping the *count* of special draws per run (see
/// OversizedVarSizedBudget below), independent of schema width or item count, avoids that blow-up.
constexpr uint64_t MODERATE_OVERSIZED_MULTIPLIER = 3;
constexpr uint64_t GROSS_OVERSIZED_MIN_MULTIPLIER = 8;
constexpr uint64_t GROSS_OVERSIZED_MAX_MULTIPLIER = 32;
/// Absolute ceiling on the gross-oversized regime so the largest BUFFER_SIZE_POOL entry (2 MiB) doesn't blow up
/// property runtime/memory (2 MiB * 32 would be 64 MiB per single value). When bufferSize itself is already
/// >= this cap, the gross-oversized regime folds into the moderate-oversized one instead.
constexpr uint64_t GROSS_OVERSIZED_ABS_CAP = 256ULL * 1024;
/// Upper bound (inclusive) on how many varsized fields in a single property run may roll a boundary/oversized
/// length; the exact per-run budget is itself randomised in [0, this]. Small enough that even the worst case
/// (all budget spent as pooled-buffer-consuming boundary draws) stays far under the smallest pooled buffer count.
constexpr int MAX_OVERSIZED_DRAWS_PER_PROPERTY = 3;

/// Shared, mutable per-property-run counter of remaining "special" (boundary/oversized) varsized draws. Threaded
/// by shared_ptr through genVarSizedLen/genVarSizedString/genAnyVec so every varsized field across an entire
/// property run decrements the same budget, instead of each field rolling independently.
using OversizedVarSizedBudget = std::shared_ptr<int>;

/// Draws a varsized payload length correlated with `bufferSize`, spending from `budget` when landing outside the
/// small [0, TestUtils::MAX_VARSIZED_LEN] case: sometimes right at the pooled-buffer boundary (off-by-one coverage),
/// sometimes moderately over one buffer's capacity, and occasionally far over it -- exercising both the
/// "doesn't fit" branch and the unpooled fallback for a range of overshoot magnitudes. Once the budget is spent,
/// always returns a small length.
rc::Gen<size_t> genVarSizedLen(uint64_t bufferSize, const OversizedVarSizedBudget& budget)
{
    return rc::gen::exec(
        [bufferSize, budget]() -> size_t
        {
            if (*budget <= 0)
            {
                return *rc::gen::inRange<size_t>(0, TestUtils::MAX_VARSIZED_LEN + 1);
            }
            /// Equal three-way split of the budget between the boundary/moderate/gross regimes.
            const auto regime = *rc::gen::inRange(0, 3);
            --*budget;
            if (regime == 0)
            {
                const auto delta = *rc::gen::inRange<int64_t>(-3, 4);
                return static_cast<size_t>(std::max<int64_t>(0, static_cast<int64_t>(bufferSize) + delta));
            }
            if (regime == 1 or bufferSize >= GROSS_OVERSIZED_ABS_CAP)
            {
                return *rc::gen::inRange<size_t>(bufferSize + 1, (bufferSize * MODERATE_OVERSIZED_MULTIPLIER) + 1);
            }
            const auto grossUpper = std::min(bufferSize * GROSS_OVERSIZED_MAX_MULTIPLIER, GROSS_OVERSIZED_ABS_CAP);
            const auto grossLower = std::min(bufferSize * GROSS_OVERSIZED_MIN_MULTIPLIER, grossUpper);
            return *rc::gen::inRange<size_t>(grossLower, grossUpper + 1);
        });
}

/// Builds a printable-ASCII string of a bufferSize-correlated length (see genVarSizedLen). Short payloads (within
/// TestUtils::MAX_VARSIZED_LEN) get varied random characters like before; longer ones are filled with a single
/// random printable character since their purpose is to stress allocation/copy sizing, not character-level
/// randomness.
rc::Gen<std::string> genVarSizedString(uint64_t bufferSize, const OversizedVarSizedBudget& budget)
{
    return rc::gen::exec(
        [bufferSize, budget]() -> std::string
        {
            const auto len = *genVarSizedLen(bufferSize, budget);
            if (len <= TestUtils::MAX_VARSIZED_LEN)
            {
                auto str = *rc::gen::container<std::string>(
                    rc::gen::inRange<char>(TestUtils::PRINTABLE_ASCII_MIN, TestUtils::PRINTABLE_ASCII_MAX));
                if (str.size() > len)
                {
                    str.resize(len);
                }
                return str;
            }
            const auto fillChar = *rc::gen::inRange<char>(TestUtils::PRINTABLE_ASCII_MIN, TestUtils::PRINTABLE_ASCII_MAX);
            /// NOLINTNEXTLINE(modernize-return-braced-init-list)
            return std::string(len, fillChar);
        });
}

/// Like genVarSizedLen, but always lands in the moderate- or gross-oversized regime (never small/boundary).
/// Used by the dedicated oversized-varsized property, which needs every generated value to definitely exceed
/// bufferSize rather than relying on genVarSizedLen's weighted mix to occasionally roll one.
rc::Gen<size_t> genOversizedVarSizedLen(uint64_t bufferSize)
{
    return rc::gen::exec(
        [bufferSize]() -> size_t
        {
            const auto useGross = bufferSize < GROSS_OVERSIZED_ABS_CAP and * rc::gen::arbitrary<bool>();
            if (useGross)
            {
                const auto grossUpper = std::min(bufferSize * GROSS_OVERSIZED_MAX_MULTIPLIER, GROSS_OVERSIZED_ABS_CAP);
                const auto grossLower = std::min(bufferSize * GROSS_OVERSIZED_MIN_MULTIPLIER, grossUpper);
                return *rc::gen::inRange<size_t>(grossLower, grossUpper + 1);
            }
            return *rc::gen::inRange<size_t>(bufferSize + 1, (bufferSize * MODERATE_OVERSIZED_MULTIPLIER) + 1);
        });
}

/// Generator for a record of arbitrary values matching the given field types. `bufferSize` is the pool buffer
/// size chosen for the current property run; VARSIZED fields draw payload lengths relative to it (see
/// genVarSizedLen), spending from the shared `budget` so oversized draws stay capped across the whole run.
rc::Gen<TestUtils::AnyVec> genAnyVec(std::vector<DataType> types, uint64_t bufferSize, const OversizedVarSizedBudget& budget)
{
    return rc::gen::exec(
        [types = std::move(types), bufferSize, budget]()
        {
            TestUtils::AnyVec result;
            result.reserve(types.size());
            for (const auto& dataType : types)
            {
                switch (dataType.type)
                {
                    case DataType::Type::UINT8:
                        result.push_back(TestUtils::genScalarAny<uint8_t>(dataType.nullable));
                        break;
                    case DataType::Type::UINT16:
                        result.push_back(TestUtils::genScalarAny<uint16_t>(dataType.nullable));
                        break;
                    case DataType::Type::UINT32:
                        result.push_back(TestUtils::genScalarAny<uint32_t>(dataType.nullable));
                        break;
                    case DataType::Type::UINT64:
                        result.push_back(TestUtils::genScalarAny<uint64_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT8:
                        result.push_back(TestUtils::genScalarAny<int8_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT16:
                        result.push_back(TestUtils::genScalarAny<int16_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT32:
                        result.push_back(TestUtils::genScalarAny<int32_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT64:
                        result.push_back(TestUtils::genScalarAny<int64_t>(dataType.nullable));
                        break;
                    case DataType::Type::FLOAT32:
                        result.push_back(TestUtils::genScalarAny<float>(dataType.nullable));
                        break;
                    case DataType::Type::FLOAT64:
                        result.push_back(TestUtils::genScalarAny<double>(dataType.nullable));
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
                            result.emplace_back(std::optional<std::string>{*genVarSizedString(bufferSize, budget)});
                            break;
                        }
                        result.emplace_back(*genVarSizedString(bufferSize, budget));
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
    return TestUtils::createSchemaFromDataTypes(types).getSizeInBytes();
}

/// Reads pagedVector.at(idx) for a rapidcheck-drawn set of indices and asserts each record equals the reference.
void verifyRandomAccess(
    TestUtils::TestablePagedVector& pagedVector, const std::vector<TestUtils::AnyVec>& reference, const std::vector<DataType>& fieldTypes)
{
    if (reference.empty())
    {
        return;
    }
    const auto indices = *rc::gen::container<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(0, reference.size()));
    for (const auto idx : indices)
    {
        const auto actual = pagedVector.readAt(idx);
        RC_ASSERT(TestUtils::anyVecsEqual(actual, reference[idx], fieldTypes));
    }
}

/// Insert N items into a PagedVector, then iterate and compare against reference.
void insertAndIterateProperty(TestUtils::EngineMode mode)
{
    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY);
    const auto oversizedBudget = std::make_shared<int>(*rc::gen::inRange(0, MAX_OVERSIZED_DRAWS_PER_PROPERTY + 1));

    NES_INFO(
        "Property insertAndIterate: fields={}, N={}, bufferSize={}, oversizedBudget={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        *oversizedBudget,
        fmt::join(fieldTypes, ", "));

    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    TestUtils::TestablePagedVector pagedVector{fieldTypes, *bufferManager, mode};

    std::vector<TestUtils::AnyVec> reference;
    reference.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes, bufferSize, oversizedBudget);
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
        RC_ASSERT(TestUtils::anyVecsEqual(actual[i], reference[i], fieldTypes));
    }
}

/// Insert N items into a PagedVector, then read each by index and compare against reference.
void insertAndReadByIndexProperty(TestUtils::EngineMode mode)
{
    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY);
    const auto oversizedBudget = std::make_shared<int>(*rc::gen::inRange(0, MAX_OVERSIZED_DRAWS_PER_PROPERTY + 1));

    NES_INFO(
        "Property insertAndReadByIndex: fields={}, N={}, bufferSize={}, oversizedBudget={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        *oversizedBudget);

    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    TestUtils::TestablePagedVector pagedVector{fieldTypes, *bufferManager, mode};

    std::vector<TestUtils::AnyVec> reference;
    reference.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes, bufferSize, oversizedBudget);
        reference.push_back(record);
        pagedVector.pushBack(record);
    }

    RC_ASSERT(pagedVector.size() == reference.size());

    verifyRandomAccess(pagedVector, reference, fieldTypes);

    for (uint64_t idx = 0; idx < reference.size(); ++idx)
    {
        auto actual = pagedVector.readAt(idx);
        RC_ASSERT(TestUtils::anyVecsEqual(actual, reference[idx], fieldTypes));
    }
}

/// Create K PagedVectors, insert items, concatMove via movePagesFrom, verify against concatenated reference.
void concatMoveProperty(TestUtils::EngineMode mode)
{
    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);

    const auto numVectors = *rc::gen::inRange<uint64_t>(1, MAX_CONCAT_VECTORS);
    const auto oversizedBudget = std::make_shared<int>(*rc::gen::inRange(0, MAX_OVERSIZED_DRAWS_PER_PROPERTY + 1));

    NES_INFO(
        "Property concatMove: fields={}, numVectors={}, bufferSize={}, oversizedBudget={}",
        fieldTypes.size(),
        numVectors,
        bufferSize,
        *oversizedBudget);

    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));

    std::vector<TestUtils::TestablePagedVector> pagedVectors;
    pagedVectors.reserve(numVectors);
    std::vector<TestUtils::AnyVec> fullReference;
    for (uint64_t vecIdx = 0; vecIdx < numVectors; ++vecIdx)
    {
        const auto itemCount = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_CONCAT_VECTOR);
        pagedVectors.emplace_back(fieldTypes, *bufferManager, mode);
        for (uint64_t i = 0; i < itemCount; ++i)
        {
            auto record = *genAnyVec(fieldTypes, bufferSize, oversizedBudget);
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
        RC_ASSERT(TestUtils::anyVecsEqual(actual[i], fullReference[i], fieldTypes));
    }
}

void concatCopyProperty(TestUtils::EngineMode mode)
{
    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);
    const auto numVectors = *rc::gen::inRange<uint64_t>(1, MAX_CONCAT_VECTORS);
    const auto oversizedBudget = std::make_shared<int>(*rc::gen::inRange(0, MAX_OVERSIZED_DRAWS_PER_PROPERTY + 1));
    NES_INFO(
        "Property concatCopy: fields={}, numVectors={}, bufferSize={}, oversizedBudget={}",
        fieldTypes.size(),
        numVectors,
        bufferSize,
        *oversizedBudget);
    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    std::vector<TestUtils::TestablePagedVector> pagedVectors;
    pagedVectors.reserve(numVectors);
    std::vector<TestUtils::AnyVec> fullReference;

    for (uint64_t vecIdx = 0; vecIdx < numVectors; ++vecIdx)
    {
        const auto itemCount = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_CONCAT_VECTOR);
        pagedVectors.emplace_back(fieldTypes, *bufferManager, mode);
        for (uint64_t i = 0; i < itemCount; ++i)
        {
            auto record = *genAnyVec(fieldTypes, bufferSize, oversizedBudget);
            fullReference.push_back(record);
            pagedVectors.back().pushBack(record);
        }
        NES_INFO("concatCopy: vector {} has {} entries", vecIdx, pagedVectors.back().size());
    }

    /// Snapshot each source vector's contents before any copy so we can verify that copyPagesFrom does not mutate or share memory with the sources.
    std::vector<std::vector<TestUtils::AnyVec>> sourceSnapshots;
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
            RC_ASSERT(TestUtils::anyVecsEqual(sourceActual[i], sourceSnapshots[vecIdx][i], fieldTypes));
        }
    }

    NES_INFO("concatCopy: merged vector has {} entries, reference has {} entries", pagedVectors[0].size(), fullReference.size());
    RC_ASSERT(pagedVectors[0].size() == fullReference.size());

    verifyRandomAccess(pagedVectors[0], fullReference, fieldTypes);

    auto actual = pagedVectors[0].toVector();
    RC_ASSERT(actual.size() == fullReference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(TestUtils::anyVecsEqual(actual[i], fullReference[i], fieldTypes));
    }
}

/// Small extra-field cap and item count kept deliberately tight: every item's VARSIZED field is forced above
/// bufferSize here (see genOversizedVarSizedLen), so a wide schema or large item count would balloon memory/time
/// the way the weighted mix in genAnyVec avoids for the general properties above.
constexpr size_t MAX_OVERSIZED_EXTRA_SCALAR_FIELDS = 3;
constexpr uint64_t MAX_ITEMS_PER_OVERSIZED_PROPERTY = 20;

/// Dedicated property targeting AbstractBufferProvider::getUnpooledBuffer fallback in makeVarSizedAllocFunction:
/// every record's VARSIZED field is forced to a length that exceeds the chosen bufferSize, so it can never fit
/// in a single pooled buffer. Unlike the general properties (whose varsized lengths only sometimes land here via
/// a weighted mix), this guarantees the fallback path runs on every push_back.
void insertOversizedVarSizedProperty(TestUtils::EngineMode mode)
{
    const auto numExtraFields = *rc::gen::inRange<size_t>(0, MAX_OVERSIZED_EXTRA_SCALAR_FIELDS + 1);
    auto fieldTypes = *TestUtils::genDataTypeSchema(SCALAR_VALUE_TYPES, numExtraFields, numExtraFields);
    const auto varSizedNullable = *rc::gen::arbitrary<bool>() ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    fieldTypes.emplace_back(DataType::Type::VARSIZED, varSizedNullable);
    const auto varSizedFieldIdx = fieldTypes.size() - 1;

    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(1, MAX_ITEMS_PER_OVERSIZED_PROPERTY + 1);

    NES_INFO(
        "Property insertOversizedVarSized: fields={}, N={}, bufferSize={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        fmt::join(fieldTypes, ", "));

    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    TestUtils::TestablePagedVector pagedVector{fieldTypes, *bufferManager, mode};

    /// genAnyVec's own draw for the VARSIZED field is immediately overwritten below with a guaranteed-oversized
    /// value, so it's kept on the small path (zero budget) to avoid wasting pooled-buffer draws before that.
    const auto noOversizedBudget = std::make_shared<int>(0);
    std::vector<TestUtils::AnyVec> reference;
    reference.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes, bufferSize, noOversizedBudget);
        const bool forceNull = fieldTypes[varSizedFieldIdx].nullable and * rc::gen::arbitrary<bool>();
        if (forceNull)
        {
            record[varSizedFieldIdx] = std::optional<std::string>{};
        }
        else
        {
            const auto len = *genOversizedVarSizedLen(bufferSize);
            const auto fillChar = *rc::gen::inRange<char>(TestUtils::PRINTABLE_ASCII_MIN, TestUtils::PRINTABLE_ASCII_MAX);
            auto str = std::string(len, fillChar);
            if (fieldTypes[varSizedFieldIdx].nullable)
            {
                record[varSizedFieldIdx] = std::optional<std::string>{std::move(str)};
            }
            else
            {
                record[varSizedFieldIdx] = std::move(str);
            }
        }
        reference.push_back(record);
        pagedVector.pushBack(record);
    }

    RC_ASSERT(pagedVector.size() == reference.size());

    verifyRandomAccess(pagedVector, reference, fieldTypes);

    auto actual = pagedVector.toVector();
    RC_ASSERT(actual.size() == reference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(TestUtils::anyVecsEqual(actual[i], reference[i], fieldTypes));
    }
}

struct SortKey
{
    size_t fieldIndex;
    bool ascending;
};

std::function<bool(const TestUtils::AnyVec&, const TestUtils::AnyVec&)>
makeAnyVecComparator(std::vector<DataType> fieldTypes, std::vector<SortKey> sortKeys)
{
    return [fieldTypes = std::move(fieldTypes), sortKeys = std::move(sortKeys)](const TestUtils::AnyVec& lhs, const TestUtils::AnyVec& rhs)
    {
        for (const auto& [fieldIndex, ascending] : sortKeys)
        {
            const auto comparison = TestUtils::compareAnyField(lhs[fieldIndex], rhs[fieldIndex], fieldTypes[fieldIndex]);
            if (comparison != 0)
            {
                return ascending ? comparison < 0 : comparison > 0;
            }
        }
        return false;
    };
}

std::pair<nautilus::val<bool>, nautilus::val<bool>>
compareNonNullRecordValues(const VarVal& lhs, const VarVal& rhs, const DataType::Type type)
{
    auto less = (lhs < rhs).getRawValueAs<nautilus::val<bool>>();
    auto equal = (lhs == rhs).getRawValueAs<nautilus::val<bool>>();
    if (type == DataType::Type::BOOLEAN)
    {
        const auto lhsValue = lhs.getRawValueAs<nautilus::val<bool>>();
        const auto rhsValue = rhs.getRawValueAs<nautilus::val<bool>>();
        less = not lhsValue and rhsValue;
        equal = lhsValue == rhsValue;
    }
    else if (type == DataType::Type::FLOAT32)
    {
        const auto lhsIsNan = nautilus::invoke(+[](float value) { return std::isnan(value); }, lhs.getRawValueAs<nautilus::val<float>>());
        const auto rhsIsNan = nautilus::invoke(+[](float value) { return std::isnan(value); }, rhs.getRawValueAs<nautilus::val<float>>());
        less = nautilus::select(lhsIsNan, nautilus::val<bool>{false}, nautilus::select(rhsIsNan, nautilus::val<bool>{true}, less));
        equal = (lhsIsNan and rhsIsNan) or (not lhsIsNan and not rhsIsNan and equal);
    }
    else if (type == DataType::Type::FLOAT64)
    {
        const auto lhsIsNan = nautilus::invoke(+[](double value) { return std::isnan(value); }, lhs.getRawValueAs<nautilus::val<double>>());
        const auto rhsIsNan = nautilus::invoke(+[](double value) { return std::isnan(value); }, rhs.getRawValueAs<nautilus::val<double>>());
        less = nautilus::select(lhsIsNan, nautilus::val<bool>{false}, nautilus::select(rhsIsNan, nautilus::val<bool>{true}, less));
        equal = (lhsIsNan and rhsIsNan) or (not lhsIsNan and not rhsIsNan and equal);
    }
    return {less, equal};
}

std::pair<nautilus::val<bool>, nautilus::val<bool>> compareRecordValues(const VarVal& lhs, const VarVal& rhs, const DataType::Type type)
{
    const auto lhsIsNull = lhs.isNull();
    const auto rhsIsNull = rhs.isNull();
    const auto bothNonNull = not lhsIsNull and not rhsIsNull;
    const auto [valueLess, valueEqual] = compareNonNullRecordValues(lhs, rhs, type);
    return {
        nautilus::select(bothNonNull, valueLess, lhsIsNull and not rhsIsNull), (lhsIsNull and rhsIsNull) or (bothNonNull and valueEqual)};
}

void sortWithRecordComparator(
    TestUtils::TestablePagedVector& pagedVector,
    const std::vector<DataType>& fieldTypes,
    const std::vector<SortKey>& sortKeys,
    TestUtils::EngineMode mode,
    Arena& arena)
{
    const auto schema = TestUtils::createSchemaFromDataTypes(fieldTypes);
    auto layout = std::make_shared<DefaultPagedVectorTupleLayout>(schema);
    std::vector<std::tuple<Record::RecordFieldIdentifier, DataType, bool>> recordSortKeys;
    recordSortKeys.reserve(sortKeys.size());
    for (const auto& [fieldIndex, ascending] : sortKeys)
    {
        recordSortKeys.emplace_back(schema[fieldIndex]->getFullyQualifiedName(), fieldTypes[fieldIndex], ascending);
    }

    auto engine = TestUtils::makeEngine(mode);
    auto module = engine.createModule();
    CompilationContext compilationContext{module};
    PagedVectorRef::registerComparator(
        compilationContext,
        "record-aware-test-comparator",
        layout,
        [recordSortKeys](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
        {
            nautilus::val<bool> result = false;
            nautilus::val<bool> equalSoFar = true;
            for (const auto& [field, type, ascending] : nautilus::static_iterable(recordSortKeys))
            {
                const auto [fieldLess, fieldEqual] = ascending ? compareRecordValues(lhs.read(field), rhs.read(field), type.type)
                                                               : compareRecordValues(rhs.read(field), lhs.read(field), type.type);
                result = nautilus::select(equalSoFar, fieldLess, result);
                equalSoFar = equalSoFar and fieldEqual;
            }
            return result;
        });
    auto comparator = PagedVectorRef::registerComparator(
        compilationContext,
        "record-aware-test-comparator",
        layout,
        [](const Record&, const Record&) -> nautilus::val<bool> { return true; });

    module.registerFunction(
        "sortPagedVectorRecords",
        std::function([layout, comparator](nautilus::val<TupleBuffer*> buffer, nautilus::val<Arena*> arenaPtr)
                      { PagedVectorRef{BorrowedNautilusBuffer::from(buffer), layout}.sort(comparator, ArenaRef{arenaPtr}); }));
    auto compiledModule = module.compile();
    compilationContext.resolveAfterCompilation(compiledModule);
    compiledModule.getFunction<void(TupleBuffer*, Arena*)>("sortPagedVectorRecords")(pagedVector.rawBuffer(), &arena);
}

void sortByRandomKeysProperty(TestUtils::EngineMode mode)
{
    const auto fieldTypes = *TestUtils::genDataTypeSchema(FIXED_WIDTH_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    RC_PRE(PagedVector::Page::getHeaderSize() + estimateSchemaSize(fieldTypes) < bufferSize);

    const auto numberOfKeys = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    std::vector<SortKey> sortKeys;
    while (sortKeys.size() < numberOfKeys)
    {
        const auto fieldIndex = *rc::gen::inRange<size_t>(0, fieldTypes.size());
        if (std::ranges::none_of(sortKeys, [fieldIndex](const auto& key) { return key.fieldIndex == fieldIndex; }))
        {
            sortKeys.emplace_back(fieldIndex, *rc::gen::arbitrary<bool>());
        }
    }

    auto expected = *rc::gen::container<std::vector<TestUtils::AnyVec>>(TestUtils::genAnyVec(fieldTypes));
    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    TestUtils::TestablePagedVector pagedVector{fieldTypes, *bufferManager, mode};
    for (const auto& record : expected)
    {
        pagedVector.pushBack(record);
    }

    std::ranges::stable_sort(expected, makeAnyVecComparator(fieldTypes, sortKeys));
    Arena arena{bufferManager};
    sortWithRecordComparator(pagedVector, fieldTypes, sortKeys, mode, arena);

    const auto actual = pagedVector.toVector();
    RC_ASSERT(actual.size() == expected.size());
    for (size_t index = 0; index < actual.size(); ++index)
    {
        RC_ASSERT(TestUtils::anyVecsEqual(actual[index], expected[index], fieldTypes));
    }
}

void oversizedVarSizedValueRoundTrip(TestUtils::EngineMode mode)
{
    constexpr size_t POOLED_BUFFER_SIZE = 4096;
    constexpr size_t PAYLOAD_SIZE = 2ULL * 1024 * 1024;
    /// The chunked unpooled-buffer manager reserves several rolling-average-sized allocations at once.
    /// Leave enough budget for that chunk when the first large payload is requested.
    constexpr size_t POOLED_BUFFER_COUNT = 512;
    auto bufferManager = DirtyBufferProvider::create(POOLED_BUFFER_SIZE, POOLED_BUFFER_COUNT);
    TestUtils::TestablePagedVector pagedVector(
        {DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}}, *bufferManager, mode);

    const std::string payload(PAYLOAD_SIZE, 'x');
    pagedVector.pushBack(TestUtils::AnyVec{payload});

    ASSERT_EQ(std::any_cast<const std::string&>(pagedVector.readAt(0).at(0)), payload);
}

void oversizedArenaValueIsAttachedWithoutCopy(TestUtils::EngineMode mode)
{
    constexpr size_t POOLED_BUFFER_SIZE = 4096;
    constexpr size_t PAYLOAD_SIZE = 2ULL * 1024 * 1024;
    /// The chunked unpooled-buffer manager reserves several rolling-average-sized allocations at once.
    /// Leave enough budget for that chunk when the first large payload is requested.
    constexpr size_t POOLED_BUFFER_COUNT = 512;
    auto bufferManager = DirtyBufferProvider::create(POOLED_BUFFER_SIZE, POOLED_BUFFER_COUNT);
    const auto schema = TestUtils::createSchemaFromDataTypes({DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}});
    auto layout = std::make_shared<DefaultPagedVectorTupleLayout>(schema);
    const auto fieldName = schema[0]->getFullyQualifiedName();

    TupleBuffer pagedVectorBuffer = bufferManager->getUnpooledBuffer(PagedVector::getMainBufferSize()).value();
    PagedVector::init(pagedVectorBuffer, bufferManager->getBufferSize(), layout->getSchema().getSizeInBytes());

    nautilus::engine::NautilusEngine engine{TestUtils::makeEngine(mode)};
    auto pushArenaValue = engine.registerFunction(std::function(
        [layout,
         fieldName](nautilus::val<TupleBuffer*> pagedVector, nautilus::val<AbstractBufferProvider*> provider, nautilus::val<Arena*> arena)
        {
            auto value = ArenaRef{arena}.allocateVariableSizedData(PAYLOAD_SIZE);
            nautilus::invoke(+[](int8_t* data) { std::memset(data, 'x', PAYLOAD_SIZE); }, value.getContent());
            Record record;
            record.write(fieldName, VarVal{value});
            PagedVectorRef{BorrowedNautilusBuffer::from(pagedVector), layout}.pushBack(record, provider);
        }));

    const std::byte* sourceAddress = nullptr;
    {
        Arena arena{bufferManager};
        pushArenaValue(&pagedVectorBuffer, bufferManager.get(), &arena);
        ASSERT_EQ(arena.unpooledBuffers.size(), 1);
        sourceAddress = arena.unpooledBuffers.front().getAvailableMemoryArea().data();

        auto page = pagedVectorBuffer.loadChildBuffer(ChildBufferIndex{0});
        auto valueBuffer = page.loadChildBuffer(ChildBufferIndex{0});
        EXPECT_EQ(valueBuffer.getControlBlock(), arena.unpooledBuffers.front().getControlBlock());
        EXPECT_EQ(valueBuffer.getAvailableMemoryArea().data(), sourceAddress);
    }

    auto page = pagedVectorBuffer.loadChildBuffer(ChildBufferIndex{0});
    auto valueBuffer = page.loadChildBuffer(ChildBufferIndex{0});
    EXPECT_EQ(valueBuffer.getAvailableMemoryArea().data(), sourceAddress);
    EXPECT_EQ(valueBuffer.getAvailableMemoryArea().front(), std::byte{'x'});
    EXPECT_EQ(valueBuffer.getAvailableMemoryArea()[PAYLOAD_SIZE - 1], std::byte{'x'});
}
} /// anonymous namespace

TEST(PagedVectorTest, OversizedVarSizedValueCompiler)
{
    oversizedVarSizedValueRoundTrip(TestUtils::EngineMode::Compiler);
}

TEST(PagedVectorTest, OversizedVarSizedValueInterpreter)
{
    oversizedVarSizedValueRoundTrip(TestUtils::EngineMode::Interpreter);
}

TEST(PagedVectorTest, OversizedArenaValueIsAttachedWithoutCopyCompiler)
{
    oversizedArenaValueIsAttachedWithoutCopy(TestUtils::EngineMode::Compiler);
}

TEST(PagedVectorTest, OversizedArenaValueIsAttachedWithoutCopyInterpreter)
{
    oversizedArenaValueIsAttachedWithoutCopy(TestUtils::EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, sortByRandomKeysCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    sortByRandomKeysProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, sortByRandomKeysInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    sortByRandomKeysProperty(TestUtils::EngineMode::Interpreter);
}

/// One RC_GTEST_PROP per (property, backend) combination so that a failure on one backend doesn't mask the other and
/// rapidcheck's shrinking chases each backend's failing input independently.
RC_GTEST_PROP(PagedVectorPropertyTest, insertAndIterateCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndIterateInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateProperty(TestUtils::EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndReadByIndexCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndReadByIndexProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndReadByIndexInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndReadByIndexProperty(TestUtils::EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatMovePagedVectorCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatMoveProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatMovePagedVectorInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatMoveProperty(TestUtils::EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatCopyPagedVectorCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatCopyProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatCopyPagedVectorInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatCopyProperty(TestUtils::EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertOversizedVarSizedCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertOversizedVarSizedProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertOversizedVarSizedInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertOversizedVarSizedProperty(TestUtils::EngineMode::Interpreter);
}

TEST(PagedVectorTest, oversizedVarSizedThrowsWhenUnpooledUnavailable)
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    constexpr uint64_t bufferSize = 4096;
    constexpr size_t numberOfBuffers = 16;
    NoOversizedUnpooledBufferProvider bufferManager{BufferManager::create(
        10 * numberOfBuffers * bufferSize,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        static_cast<uint32_t>(bufferSize),
        std::make_shared<NesDefaultMemoryAllocator>())};

    const std::vector<DataType> fieldTypes{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}};
    TestUtils::TestablePagedVector pagedVector{fieldTypes, bufferManager, TestUtils::EngineMode::Interpreter};

    TestUtils::AnyVec record;
    /// oversized -> unpooled fallback -> nullopt -> throw
    record.emplace_back(std::string(bufferSize * 2, 'x'));
    ASSERT_EXCEPTION_ERRORCODE(pagedVector.pushBack(record), ErrorCode::BufferAllocationFailure);
}

}
