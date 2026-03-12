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

#include <cstdint>
#include <memory>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <NautilusTestUtils.hpp>
#include <PropertyTestUtils.hpp>
#include <val.hpp>

#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

namespace NES
{

/// NOLINTNEXTLINE(google-build-using-namespace) test code benefits from concise access to test utilities
using namespace TestUtils;

namespace
{

// ===================== TestablePagedVector (inlined) =====================

/// Mirrors std::vector<AnyVec> but internally operates on a real PagedVector via direct PagedVectorRef calls.
class TestablePagedVector
{
public:
    TestablePagedVector(
        const std::vector<DataType>& fieldTypes,
        MemoryLayoutType layoutType,
        uint64_t pageSize,
        BufferManager& bufferManager)
        : dataTypes_(fieldTypes), bufferManager_(bufferManager)
    {
        const auto schema = NautilusTestUtils::createSchemaFromDataTypes(dataTypes_);
        projections_ = schema.getFieldNames();
        pagedVectorBufferRef_ = LowerSchemaProvider::lowerSchema(pageSize, schema, layoutType);
    }

    void insert(const AnyVec& record)
    {
        auto rec = anyVecToRecord(record, projections_, dataTypes_);
        const PagedVectorRef pvRef(nautilus::val<PagedVector*>(&pagedVector_), pagedVectorBufferRef_);
        pvRef.writeRecord(rec, nautilus::val<AbstractBufferProvider*>(&bufferManager_));
    }

    AnyVec readAt(uint64_t index)
    {
        const PagedVectorRef pvRef(nautilus::val<PagedVector*>(&pagedVector_), pagedVectorBufferRef_);
        auto record = pvRef.readRecord(nautilus::val<uint64_t>(index), projections_);
        return recordToAnyVec(record, projections_, dataTypes_);
    }

    std::vector<AnyVec> toVector()
    {
        const PagedVectorRef pvRef(nautilus::val<PagedVector*>(&pagedVector_), pagedVectorBufferRef_);
        std::vector<AnyVec> result;
        for (auto it = pvRef.begin(projections_); it != pvRef.end(projections_); ++it)
        {
            auto record = *it;
            result.push_back(recordToAnyVec(record, projections_, dataTypes_));
        }
        return result;
    }

    void concat(TestablePagedVector& other) { pagedVector_.moveAllPages(other.pagedVector_); }

    uint64_t size() const { return pagedVector_.getTotalNumberOfEntries(); }
    PagedVector& raw() { return pagedVector_; }

private:
    std::vector<DataType> dataTypes_;
    PagedVector pagedVector_;
    BufferManager& bufferManager_;
    std::shared_ptr<TupleBufferRef> pagedVectorBufferRef_;
    std::vector<Record::RecordFieldIdentifier> projections_;
};

// ===================== Property Scenarios =====================

/// Insert N items into a PagedVector, then iterate and compare against reference.
void insertAndIterateProperty(MemoryLayoutType layoutType)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, 128);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(50, 501);
    const auto pageSize = *rc::gen::inRange<uint64_t>(64, 1048577);

    NES_INFO(
        "Property insertAndIterate: fields={}, N={}, pageSize={}, layout={}",
        fieldTypes.size(), numberOfItems, pageSize,
        magic_enum::enum_name(layoutType));

    auto bufferManager = BufferManager::create();
    TestablePagedVector pv(fieldTypes, layoutType, pageSize, *bufferManager);

    std::vector<AnyVec> reference;
    reference.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes);
        reference.push_back(record);
        pv.insert(record);
    }

    NES_INFO("insertAndIterate: PagedVector has {} entries", pv.size());
    RC_ASSERT(pv.size() == reference.size());

    auto actual = pv.toVector();
    RC_ASSERT(actual.size() == reference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(anyVecsEqual(actual[i], reference[i], fieldTypes));
    }
}

/// Insert N items into a PagedVector, then read each by index and compare against reference.
void insertAndReadByIndexProperty(MemoryLayoutType layoutType)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, 128);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(50, 501);
    const auto pageSize = *rc::gen::inRange<uint64_t>(64, 1048577);

    NES_INFO(
        "Property insertAndReadByIndex: fields={}, N={}, pageSize={}, layout={}",
        fieldTypes.size(), numberOfItems, pageSize,
        magic_enum::enum_name(layoutType));

    auto bufferManager = BufferManager::create();
    TestablePagedVector pv(fieldTypes, layoutType, pageSize, *bufferManager);

    std::vector<AnyVec> reference;
    reference.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes);
        reference.push_back(record);
        pv.insert(record);
    }

    RC_ASSERT(pv.size() == reference.size());

    for (uint64_t idx = 0; idx < reference.size(); ++idx)
    {
        auto actual = pv.readAt(idx);
        RC_ASSERT(anyVecsEqual(actual, reference[idx], fieldTypes));
    }
}

/// Create K PagedVectors, insert items, concat via moveAllPages, verify against concatenated reference.
void concatProperty(MemoryLayoutType layoutType)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, 128);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);

    const auto numVectors = *rc::gen::inRange<uint64_t>(2, 5);
    const auto pageSize = *rc::gen::inRange<uint64_t>(64, 1048577);

    NES_INFO(
        "Property concat: fields={}, numVectors={}, pageSize={}, layout={}",
        fieldTypes.size(), numVectors, pageSize,
        magic_enum::enum_name(layoutType));

    auto bufferManager = BufferManager::create();

    std::vector<TestablePagedVector> pagedVectors;
    std::vector<AnyVec> fullReference;

    for (uint64_t v = 0; v < numVectors; ++v)
    {
        const auto itemCount = *rc::gen::inRange<uint64_t>(50, 201);
        pagedVectors.emplace_back(fieldTypes, layoutType, pageSize, *bufferManager);
        for (uint64_t i = 0; i < itemCount; ++i)
        {
            auto record = *genAnyVec(fieldTypes);
            fullReference.push_back(record);
            pagedVectors.back().insert(record);
        }
        NES_INFO("concat: vector {} has {} entries", v, pagedVectors.back().size());
    }

    for (uint64_t v = 1; v < numVectors; ++v)
    {
        pagedVectors[0].concat(pagedVectors[v]);
        RC_ASSERT(pagedVectors[v].raw().getNumberOfPages() == 0);
        RC_ASSERT(pagedVectors[v].size() == 0);
    }

    NES_INFO("concat: merged vector has {} entries, reference has {} entries", pagedVectors[0].size(), fullReference.size());
    RC_ASSERT(pagedVectors[0].size() == fullReference.size());

    auto actual = pagedVectors[0].toVector();
    RC_ASSERT(actual.size() == fullReference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(anyVecsEqual(actual[i], fullReference[i], fieldTypes));
    }
}

} /// anonymous namespace

// ===================== Test Cases =====================

TEST(PagedVectorTest, readAtValidAccess)
{
    Logger::setupLogging("PagedVectorTest.log", LogLevel::LOG_DEBUG);
    const std::vector<DataType> fieldTypes = {{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}};
    auto bufferManager = BufferManager::create();

    TestablePagedVector pv(fieldTypes, MemoryLayoutType::ROW_LAYOUT, 4096, *bufferManager);
    pv.insert({std::any(int32_t{42})});
    pv.insert({std::any(int32_t{99})});
    ASSERT_EQ(pv.size(), 2);

    auto val0 = pv.readAt(0);
    ASSERT_EQ(std::any_cast<int32_t>(val0[0]), 42);

    auto val1 = pv.readAt(1);
    ASSERT_EQ(std::any_cast<int32_t>(val1[0]), 99);
}

// ===================== Property Tests =====================

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndIterateRow, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateProperty(MemoryLayoutType::ROW_LAYOUT);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndIterateColumnar, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateProperty(MemoryLayoutType::COLUMNAR_LAYOUT);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndReadByIndexRow, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndReadByIndexProperty(MemoryLayoutType::ROW_LAYOUT);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatRow, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatProperty(MemoryLayoutType::ROW_LAYOUT);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatColumnar, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatProperty(MemoryLayoutType::COLUMNAR_LAYOUT);
}

}
