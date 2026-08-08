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
#include <Interface/PagedVector/PagedVector.hpp>
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
#include <BaseUnitTest.hpp>
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
constexpr uint64_t MAX_VARSIZED_MEMORY_BUDGET = 64ULL * 1024 * 1024;

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

    NES_INFO(
        "Property insertAndIterate: fields={}, N={}, bufferSize={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        fmt::join(fieldTypes, ", "));

    const auto initialVarSizedMemoryBudget = *rc::gen::inRange<uint64_t>(0, MAX_VARSIZED_MEMORY_BUDGET + 1);
    auto varSizedMemoryBudget = std::make_shared<uint64_t>(initialVarSizedMemoryBudget);
    auto reference
        = *rc::gen::container<std::vector<TestUtils::AnyVec>>(numberOfItems, TestUtils::genAnyVec(fieldTypes, varSizedMemoryBudget));

    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize, initialVarSizedMemoryBudget));
    TestUtils::TestablePagedVector pagedVector{fieldTypes, *bufferManager, mode};
    for (const auto& record : reference)
    {
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

    NES_INFO("Property insertAndReadByIndex: fields={}, N={}, bufferSize={}", fieldTypes.size(), numberOfItems, bufferSize);

    const auto initialVarSizedMemoryBudget = *rc::gen::inRange<uint64_t>(0, MAX_VARSIZED_MEMORY_BUDGET + 1);
    auto varSizedMemoryBudget = std::make_shared<uint64_t>(initialVarSizedMemoryBudget);
    auto reference
        = *rc::gen::container<std::vector<TestUtils::AnyVec>>(numberOfItems, TestUtils::genAnyVec(fieldTypes, varSizedMemoryBudget));

    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize, initialVarSizedMemoryBudget));
    TestUtils::TestablePagedVector pagedVector{fieldTypes, *bufferManager, mode};
    for (const auto& record : reference)
    {
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

    NES_INFO("Property concatMove: fields={}, numVectors={}, bufferSize={}", fieldTypes.size(), numVectors, bufferSize);

    std::vector<std::vector<TestUtils::AnyVec>> references;
    references.reserve(numVectors);
    std::vector<TestUtils::AnyVec> fullReference;
    const auto initialVarSizedMemoryBudget = *rc::gen::inRange<uint64_t>(0, MAX_VARSIZED_MEMORY_BUDGET + 1);
    auto varSizedMemoryBudget = std::make_shared<uint64_t>(initialVarSizedMemoryBudget);
    for (uint64_t vecIdx = 0; vecIdx < numVectors; ++vecIdx)
    {
        const auto itemCount = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_CONCAT_VECTOR);
        references.push_back(
            *rc::gen::container<std::vector<TestUtils::AnyVec>>(itemCount, TestUtils::genAnyVec(fieldTypes, varSizedMemoryBudget)));
        fullReference.insert(fullReference.end(), references.back().begin(), references.back().end());
    }

    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize, initialVarSizedMemoryBudget));
    std::vector<TestUtils::TestablePagedVector> pagedVectors;
    pagedVectors.reserve(numVectors);
    for (uint64_t vecIdx = 0; vecIdx < numVectors; ++vecIdx)
    {
        pagedVectors.emplace_back(fieldTypes, *bufferManager, mode);
        for (const auto& record : references[vecIdx])
        {
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
    NES_INFO("Property concatCopy: fields={}, numVectors={}, bufferSize={}", fieldTypes.size(), numVectors, bufferSize);

    std::vector<std::vector<TestUtils::AnyVec>> references;
    references.reserve(numVectors);
    std::vector<TestUtils::AnyVec> fullReference;
    const auto initialVarSizedMemoryBudget = *rc::gen::inRange<uint64_t>(0, MAX_VARSIZED_MEMORY_BUDGET + 1);
    auto varSizedMemoryBudget = std::make_shared<uint64_t>(initialVarSizedMemoryBudget);
    for (uint64_t vecIdx = 0; vecIdx < numVectors; ++vecIdx)
    {
        const auto itemCount = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_CONCAT_VECTOR);
        references.push_back(
            *rc::gen::container<std::vector<TestUtils::AnyVec>>(itemCount, TestUtils::genAnyVec(fieldTypes, varSizedMemoryBudget)));
        fullReference.insert(fullReference.end(), references.back().begin(), references.back().end());
    }

    auto bufferManager = DirtyBufferProvider::create(bufferSize, TestUtils::pooledBufferCountFor(bufferSize, initialVarSizedMemoryBudget));
    std::vector<TestUtils::TestablePagedVector> pagedVectors;
    pagedVectors.reserve(numVectors);
    for (uint64_t vecIdx = 0; vecIdx < numVectors; ++vecIdx)
    {
        pagedVectors.emplace_back(fieldTypes, *bufferManager, mode);
        for (const auto& record : references[vecIdx])
        {
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

} /// anonymous namespace

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
