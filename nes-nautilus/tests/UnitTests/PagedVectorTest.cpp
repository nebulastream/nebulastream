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

#include <any>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/Engine.hpp>
#include <NautilusTestUtils.hpp>
#include <PropertyTestUtils.hpp>
#include <val.hpp>
#include <val_details.hpp>
#include <val_ptr.hpp>

#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

namespace NES
{

/// NOLINTNEXTLINE(google-build-using-namespace) test code benefits from concise access to test utilities
using namespace TestUtils;

namespace
{

/// RC generator for MemoryLayoutType
rc::Gen<MemoryLayoutType> genMemoryLayout()
{
    return rc::gen::element(MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::COLUMNAR_LAYOUT);
}

/// Sets up a nautilus engine for the given backend.
std::unique_ptr<nautilus::engine::NautilusEngine> createEngine(ExecutionMode backend)
{
    nautilus::engine::Options options;
    const bool compilation = (backend == ExecutionMode::COMPILER);
    options.setOption("engine.Compilation", compilation);
    return std::make_unique<nautilus::engine::NautilusEngine>(options);
}

/// Compiles a function that inserts all records from an input buffer into a PagedVector.
auto compileInsertFunction(
    const nautilus::engine::NautilusEngine& engine,
    const std::shared_ptr<TupleBufferRef>& inputBufferRef,
    const std::shared_ptr<TupleBufferRef>& pagedVectorBufferRef,
    const std::vector<Record::RecordFieldIdentifier>& projections)
{
    /// NOLINTBEGIN(performance-unnecessary-value-param) shared_ptrs are captured by value intentionally for nautilus lambda registration
    return engine.registerFunction(std::function(
        [=](nautilus::val<TupleBuffer*> inputBufferPtr,
            nautilus::val<AbstractBufferProvider*> bufferProviderVal,
            nautilus::val<PagedVector*> pagedVectorVal)
        {
            const RecordBuffer recordBuffer(inputBufferPtr);
            const PagedVectorRef pagedVectorRef(pagedVectorVal, pagedVectorBufferRef);

            for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
            {
                const auto record = inputBufferRef->readRecord(projections, recordBuffer, i);
                pagedVectorRef.writeRecord(record, bufferProviderVal);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)
}

/// Compiles a function that reads all records from a PagedVector via iteration into an output buffer.
auto compileIterateFunction(
    const nautilus::engine::NautilusEngine& engine,
    const std::shared_ptr<TupleBufferRef>& outputBufferRef,
    const std::shared_ptr<TupleBufferRef>& pagedVectorBufferRef,
    const std::vector<Record::RecordFieldIdentifier>& projections)
{
    /// NOLINTBEGIN(performance-unnecessary-value-param) shared_ptrs are captured by value intentionally for nautilus lambda registration
    return engine.registerFunction(std::function(
        [=](nautilus::val<TupleBuffer*> outputBufferPtr,
            nautilus::val<AbstractBufferProvider*> bufferProviderVal,
            nautilus::val<PagedVector*> pagedVectorVal)
        {
            RecordBuffer recordBuffer(outputBufferPtr);
            const PagedVectorRef pagedVectorRef(pagedVectorVal, pagedVectorBufferRef);

            nautilus::val<uint64_t> numberOfTuples = 0;
            for (auto it = pagedVectorRef.begin(projections); it != pagedVectorRef.end(projections); ++it)
            {
                auto record = *it;
                outputBufferRef->writeRecord(numberOfTuples, recordBuffer, record, bufferProviderVal);
                numberOfTuples = numberOfTuples + 1;
                recordBuffer.setNumRecords(numberOfTuples);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)
}

/// Compiles a function that reads a single record from a PagedVector by index.
auto compileReadByIndexFunction(
    const nautilus::engine::NautilusEngine& engine,
    const std::shared_ptr<TupleBufferRef>& outputBufferRef,
    const std::shared_ptr<TupleBufferRef>& pagedVectorBufferRef,
    const std::vector<Record::RecordFieldIdentifier>& projections)
{
    /// NOLINTBEGIN(performance-unnecessary-value-param) shared_ptrs are captured by value intentionally for nautilus lambda registration
    return engine.registerFunction(std::function(
        [=](nautilus::val<TupleBuffer*> outputBufferPtr,
            nautilus::val<AbstractBufferProvider*> bufferProviderVal,
            nautilus::val<PagedVector*> pagedVectorVal,
            nautilus::val<uint64_t> index)
        {
            RecordBuffer recordBuffer(outputBufferPtr);
            const PagedVectorRef pagedVectorRef(pagedVectorVal, pagedVectorBufferRef);

            auto record = pagedVectorRef.readRecord(index, projections);
            nautilus::val<uint64_t> pos = 0;
            outputBufferRef->writeRecord(pos, recordBuffer, record, bufferProviderVal);
            recordBuffer.setNumRecords(1);
        }));
    /// NOLINTEND(performance-unnecessary-value-param)
}

/// Builds the reference vector from input buffers by reading all records in order.
std::vector<std::vector<std::any>> buildReference(
    const std::vector<TupleBuffer>& inputBuffers,
    const std::shared_ptr<TupleBufferRef>& inputBufferRef,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const std::vector<DataType::Type>& types)
{
    std::vector<std::vector<std::any>> reference;
    for (const auto& buffer : inputBuffers)
    {
        const RecordBuffer recordBuffer(nautilus::val<const TupleBuffer*>(std::addressof(buffer)));
        for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
        {
            auto record = inputBufferRef->readRecord(projections, recordBuffer, i);
            reference.push_back(recordToAnyVec(record, projections, types));
        }
    }
    return reference;
}

/// ==================== Property Functions ====================

constexpr size_t MAX_FIELDS_INTERPRETER = 128;
constexpr size_t MAX_FIELDS_COMPILER = 32;

/// Insert N items into a PagedVector, then iterate and compare against reference.
void insertAndIterateProperty(ExecutionMode backend, size_t maxFields)
{
    const auto fieldTypes = *genTypeSchema(ALL_VALUE_TYPES, 1, maxFields);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(500, 5001);
    const auto pageSize = *rc::gen::inRange<uint64_t>(64, 1048577);
    const auto layoutType = *genMemoryLayout();

    NES_INFO(
        "Property insertAndIterate: fields={}, N={}, pageSize={}, layout={}, backend={}",
        fieldTypes.size(),
        numberOfItems,
        pageSize,
        magic_enum::enum_name(layoutType),
        magic_enum::enum_name(backend));

    auto bufferManager = BufferManager::create();
    auto engine = createEngine(backend);

    const auto schema = TestUtils::NautilusTestUtils::createSchemaFromBasicTypes(fieldTypes);
    const auto projections = schema.getFieldNames();
    TestUtils::NautilusTestUtils testUtils;
    const auto inputBuffers = testUtils.createMonotonicallyIncreasingValues(schema, layoutType, numberOfItems, *bufferManager);

    const auto inputBufferRef = LowerSchemaProvider::lowerSchema(inputBuffers[0].getBufferSize(), schema, layoutType);
    const auto pagedVectorBufferRef = LowerSchemaProvider::lowerSchema(pageSize, schema, layoutType);

    /// Insert all records into PagedVector
    PagedVector pagedVector;
    auto insertFn = compileInsertFunction(*engine, inputBufferRef, pagedVectorBufferRef, projections);
    for (auto buf : inputBuffers)
    {
        insertFn(std::addressof(buf), std::addressof(*bufferManager), std::addressof(pagedVector));
    }

    /// Build reference
    const auto reference = buildReference(inputBuffers, inputBufferRef, projections, fieldTypes);
    NES_INFO(
        "insertAndIterate: PagedVector has {} entries, reference has {} entries", pagedVector.getTotalNumberOfEntries(), reference.size());
    RC_ASSERT(pagedVector.getTotalNumberOfEntries() == reference.size());

    /// Iterate PagedVector and compare
    const auto totalEntries = pagedVector.getTotalNumberOfEntries();
    auto outputBufferOpt = bufferManager->getUnpooledBuffer(totalEntries * schema.getSizeOfSchemaInBytes());
    RC_ASSERT(outputBufferOpt.has_value());
    auto outputBuffer = outputBufferOpt.value();

    const auto outputBufferRef = LowerSchemaProvider::lowerSchema(outputBuffer.getBufferSize(), schema, layoutType);
    auto iterateFn = compileIterateFunction(*engine, outputBufferRef, pagedVectorBufferRef, projections);
    iterateFn(std::addressof(outputBuffer), std::addressof(*bufferManager), std::addressof(pagedVector));

    RC_ASSERT(outputBuffer.getNumberOfTuples() == totalEntries);

    const RecordBuffer recordBufferOutput(nautilus::val<const TupleBuffer*>(std::addressof(outputBuffer)));
    for (nautilus::val<uint64_t> i = 0; i < outputBuffer.getNumberOfTuples(); ++i)
    {
        auto record = outputBufferRef->readRecord(projections, recordBufferOutput, i);
        auto actual = recordToAnyVec(record, projections, fieldTypes);
        const auto idx = nautilus::details::RawValueResolver<uint64_t>::getRawValue(i);
        RC_ASSERT(anyVecsEqual(actual, reference[idx], fieldTypes));
    }
}

/// Insert N items into a PagedVector, then read each by index and compare against reference.
void insertAndReadByIndexProperty(ExecutionMode backend, size_t maxFields)
{
    const auto fieldTypes = *genTypeSchema(ALL_VALUE_TYPES, 1, maxFields);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(500, 5001);
    const auto pageSize = *rc::gen::inRange<uint64_t>(64, 1048577);
    const auto layoutType = *genMemoryLayout();

    NES_INFO(
        "Property insertAndReadByIndex: fields={}, N={}, pageSize={}, layout={}, backend={}",
        fieldTypes.size(),
        numberOfItems,
        pageSize,
        magic_enum::enum_name(layoutType),
        magic_enum::enum_name(backend));

    auto bufferManager = BufferManager::create();
    auto engine = createEngine(backend);

    const auto schema = TestUtils::NautilusTestUtils::createSchemaFromBasicTypes(fieldTypes);
    const auto projections = schema.getFieldNames();
    TestUtils::NautilusTestUtils testUtils;
    const auto inputBuffers = testUtils.createMonotonicallyIncreasingValues(schema, layoutType, numberOfItems, *bufferManager);

    const auto inputBufferRef = LowerSchemaProvider::lowerSchema(inputBuffers[0].getBufferSize(), schema, layoutType);
    const auto pagedVectorBufferRef = LowerSchemaProvider::lowerSchema(pageSize, schema, layoutType);

    /// Insert all records into PagedVector
    PagedVector pagedVector;
    auto insertFn = compileInsertFunction(*engine, inputBufferRef, pagedVectorBufferRef, projections);
    for (auto buf : inputBuffers)
    {
        insertFn(std::addressof(buf), std::addressof(*bufferManager), std::addressof(pagedVector));
    }

    /// Build reference
    const auto reference = buildReference(inputBuffers, inputBufferRef, projections, fieldTypes);
    RC_ASSERT(pagedVector.getTotalNumberOfEntries() == reference.size());

    /// Read each entry by index and compare
    auto singleOutputOpt = bufferManager->getUnpooledBuffer(schema.getSizeOfSchemaInBytes());
    RC_ASSERT(singleOutputOpt.has_value());
    auto singleOutputBuffer = singleOutputOpt.value();
    const auto singleOutputRef = LowerSchemaProvider::lowerSchema(singleOutputBuffer.getBufferSize(), schema, layoutType);
    auto readByIndexFn = compileReadByIndexFunction(*engine, singleOutputRef, pagedVectorBufferRef, projections);

    for (uint64_t idx = 0; idx < reference.size(); ++idx)
    {
        singleOutputBuffer.setNumberOfTuples(0);
        readByIndexFn(std::addressof(singleOutputBuffer), std::addressof(*bufferManager), std::addressof(pagedVector), idx);
        RC_ASSERT(singleOutputBuffer.getNumberOfTuples() == 1);

        const RecordBuffer recordBufferOutput(nautilus::val<const TupleBuffer*>(std::addressof(singleOutputBuffer)));
        nautilus::val<uint64_t> pos = 0;
        auto record = singleOutputRef->readRecord(projections, recordBufferOutput, pos);
        auto actual = recordToAnyVec(record, projections, fieldTypes);
        RC_ASSERT(anyVecsEqual(actual, reference[idx], fieldTypes));
    }
}

/// Create K PagedVectors, insert items, concat via moveAllPages, verify against concatenated reference.
void concatProperty(ExecutionMode backend, size_t maxFields)
{
    const auto fieldTypes = *genTypeSchema(ALL_VALUE_TYPES, 1, maxFields);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);

    const auto numVectors = *rc::gen::inRange<uint64_t>(2, 5);
    const auto pageSize = *rc::gen::inRange<uint64_t>(64, 1048577);
    const auto layoutType = *genMemoryLayout();

    NES_INFO(
        "Property concat: fields={}, numVectors={}, pageSize={}, layout={}, backend={}",
        fieldTypes.size(),
        numVectors,
        pageSize,
        magic_enum::enum_name(layoutType),
        magic_enum::enum_name(backend));

    auto bufferManager = BufferManager::create();
    auto engine = createEngine(backend);

    const auto schema = TestUtils::NautilusTestUtils::createSchemaFromBasicTypes(fieldTypes);
    const auto projections = schema.getFieldNames();

    /// Generate per-vector item counts
    std::vector<uint64_t> itemCounts;
    itemCounts.reserve(numVectors);
    for (uint64_t v = 0; v < numVectors; ++v)
    {
        itemCounts.push_back(*rc::gen::inRange<uint64_t>(100, 1001));
    }

    /// Create input data and PagedVectors for each
    std::vector<std::unique_ptr<PagedVector>> pagedVectors;
    std::vector<std::vector<std::any>> fullReference;
    TestUtils::NautilusTestUtils testUtils;

    for (uint64_t v = 0; v < numVectors; ++v)
    {
        auto inputBuffers = testUtils.createMonotonicallyIncreasingValues(schema, layoutType, itemCounts[v], *bufferManager);
        const auto inputBufferRef = LowerSchemaProvider::lowerSchema(inputBuffers[0].getBufferSize(), schema, layoutType);
        const auto pagedVectorBufferRef = LowerSchemaProvider::lowerSchema(pageSize, schema, layoutType);

        pagedVectors.emplace_back(std::make_unique<PagedVector>());
        auto insertFn = compileInsertFunction(*engine, inputBufferRef, pagedVectorBufferRef, projections);
        for (auto buf : inputBuffers)
        {
            insertFn(std::addressof(buf), std::addressof(*bufferManager), pagedVectors.back().get());
        }

        /// Build per-vector reference and append to full reference
        auto ref = buildReference(inputBuffers, inputBufferRef, projections, fieldTypes);
        fullReference.insert(fullReference.end(), ref.begin(), ref.end());

        NES_INFO("concat: vector {} has {} entries", v, pagedVectors.back()->getTotalNumberOfEntries());
    }

    /// Concat all into first vector
    for (uint64_t v = 1; v < numVectors; ++v)
    {
        pagedVectors[0]->moveAllPages(*pagedVectors[v]);
        RC_ASSERT(pagedVectors[v]->getNumberOfPages() == 0);
        RC_ASSERT(pagedVectors[v]->getTotalNumberOfEntries() == 0);
    }

    NES_INFO(
        "concat: merged vector has {} entries, reference has {} entries", pagedVectors[0]->getTotalNumberOfEntries(), fullReference.size());
    RC_ASSERT(pagedVectors[0]->getTotalNumberOfEntries() == fullReference.size());

    /// Iterate merged vector and compare
    const auto totalEntries = pagedVectors[0]->getTotalNumberOfEntries();
    auto outputBufferOpt = bufferManager->getUnpooledBuffer(totalEntries * schema.getSizeOfSchemaInBytes());
    RC_ASSERT(outputBufferOpt.has_value());
    auto outputBuffer = outputBufferOpt.value();

    const auto outputBufferRef = LowerSchemaProvider::lowerSchema(outputBuffer.getBufferSize(), schema, layoutType);
    const auto pagedVectorBufferRef = LowerSchemaProvider::lowerSchema(pageSize, schema, layoutType);
    auto iterateFn = compileIterateFunction(*engine, outputBufferRef, pagedVectorBufferRef, projections);
    iterateFn(std::addressof(outputBuffer), std::addressof(*bufferManager), pagedVectors[0].get());

    RC_ASSERT(outputBuffer.getNumberOfTuples() == totalEntries);

    const RecordBuffer recordBufferOutput(nautilus::val<const TupleBuffer*>(std::addressof(outputBuffer)));
    for (nautilus::val<uint64_t> i = 0; i < outputBuffer.getNumberOfTuples(); ++i)
    {
        auto record = outputBufferRef->readRecord(projections, recordBufferOutput, i);
        auto actual = recordToAnyVec(record, projections, fieldTypes);
        const auto idx = nautilus::details::RawValueResolver<uint64_t>::getRawValue(i);
        RC_ASSERT(anyVecsEqual(actual, fullReference[idx], fieldTypes));
    }
}

} /// anonymous namespace

/// ==================== Test Cases ====================

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndIterateInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateProperty(ExecutionMode::INTERPRETER, MAX_FIELDS_INTERPRETER);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndIterateCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateProperty(ExecutionMode::COMPILER, MAX_FIELDS_COMPILER);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndReadByIndexInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndReadByIndexProperty(ExecutionMode::INTERPRETER, MAX_FIELDS_INTERPRETER);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndReadByIndexCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndReadByIndexProperty(ExecutionMode::COMPILER, MAX_FIELDS_COMPILER);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatInterpreter, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatProperty(ExecutionMode::INTERPRETER, MAX_FIELDS_INTERPRETER);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatCompiler, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatProperty(ExecutionMode::COMPILER, MAX_FIELDS_COMPILER);
}

}
