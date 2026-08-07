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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Interface/BTree/BTree.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed by rapidcheck/gtest.h
#include <DataStructureTestUtils.hpp>
#include <Engine.hpp>
#include <TestableBTree.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

#include <rapidcheck.h>
#include <fmt/ranges.h>
#include <rapidcheck/gtest.h>

namespace NES
{
namespace
{
/// Small pages force frequent node splits; the large page covers the no-split case.
constexpr std::array<uint64_t, 4> BUFFER_SIZE_POOL = {128, 512, 4096, 2ULL * 1024 * 1024};
constexpr uint64_t MAX_VARSIZED_MEMORY_BUDGET = 64ULL * 1024 * 1024;

template <typename T>
int compareValues(const std::any& lhs, const std::any& rhs, const bool nullable)
{
    const auto compareNonNull = [](const T& left, const T& right)
    {
        if constexpr (std::is_floating_point_v<T>)
        {
            const auto leftIsNan = std::isnan(left);
            const auto rightIsNan = std::isnan(right);
            if (leftIsNan or rightIsNan)
            {
                if (leftIsNan == rightIsNan)
                {
                    return 0;
                }
                return leftIsNan ? 1 : -1;
            }
        }
        if (left < right)
        {
            return -1;
        }
        if (left > right)
        {
            return 1;
        }
        return 0;
    };

    if (not nullable)
    {
        return compareNonNull(std::any_cast<const T&>(lhs), std::any_cast<const T&>(rhs));
    }
    const auto& left = std::any_cast<const std::optional<T>&>(lhs);
    const auto& right = std::any_cast<const std::optional<T>&>(rhs);
    if (not left or not right)
    {
        if (left.has_value() == right.has_value())
        {
            return 0;
        }
        return left ? 1 : -1;
    }
    return compareNonNull(*left, *right);
}

int compareFields(const std::any& lhs, const std::any& rhs, const DataType type)
{
    switch (type.type)
    {
        case DataType::Type::UINT8:
            return compareValues<uint8_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT16:
            return compareValues<uint16_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT32:
            return compareValues<uint32_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT64:
            return compareValues<uint64_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT8:
            return compareValues<int8_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT16:
            return compareValues<int16_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT32:
            return compareValues<int32_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT64:
            return compareValues<int64_t>(lhs, rhs, type.nullable);
        case DataType::Type::FLOAT32:
            return compareValues<float>(lhs, rhs, type.nullable);
        case DataType::Type::FLOAT64:
            return compareValues<double>(lhs, rhs, type.nullable);
        case DataType::Type::VARSIZED:
            return compareValues<std::string>(lhs, rhs, type.nullable);
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw std::logic_error("Unsupported BTree property-test type");
    }
    std::unreachable();
}

struct AnyVecLess
{
    const std::vector<DataType>* fieldTypes;

    bool operator()(const TestUtils::AnyVec& lhs, const TestUtils::AnyVec& rhs) const
    {
        for (size_t index = 0; index < fieldTypes->size(); ++index)
        {
            if (const auto comparison = compareFields(lhs[index], rhs[index], (*fieldTypes)[index]); comparison != 0)
            {
                return comparison < 0;
            }
        }
        return false;
    }
};

bool recordsEqual(const TestUtils::AnyVec& lhs, const TestUtils::AnyVec& rhs, const std::vector<DataType>& fieldTypes)
{
    if (lhs.size() != rhs.size() or lhs.size() != fieldTypes.size())
    {
        return false;
    }
    for (size_t index = 0; index < fieldTypes.size(); ++index)
    {
        if (compareFields(lhs[index], rhs[index], fieldTypes[index]) != 0)
        {
            return false;
        }
    }
    return true;
}

bool schemaFitsBTreePage(const std::vector<DataType>& fieldTypes, const uint64_t bufferSize)
{
    /// BTree nodes contain an aligned 16-byte header, at least three keys, and four child indices.
    const auto entrySize = TestUtils::createSchemaFromDataTypes(fieldTypes).getSizeInBytes();
    return 16 + (3 * entrySize) + (4 * sizeof(uint32_t)) <= bufferSize;
}

std::vector<TestUtils::AnyVec> generateRecords(
    const std::vector<DataType>& fieldTypes, const uint64_t numberOfItems, const TestUtils::VarSizedMemoryBudget& varSizedMemoryBudget)
{
    auto records
        = *rc::gen::container<std::vector<TestUtils::AnyVec>>(numberOfItems, TestUtils::genAnyVec(fieldTypes, varSizedMemoryBudget))
               .as("records");
    /// Make comparator-equivalent records common rather than relying on collisions in full-width random scalars.
    for (size_t index = 7; index < records.size(); index += 7)
    {
        records[index] = records[index / 2];
    }
    return records;
}

void sortOracle(std::vector<TestUtils::AnyVec>& records, const std::vector<DataType>& fieldTypes)
{
    std::ranges::sort(records, AnyVecLess{&fieldTypes});
}

void verifyAtExhaustively(
    TestUtils::TestableBTree& tree, const std::vector<TestUtils::AnyVec>& expected, const std::vector<DataType>& fieldTypes)
{
    RC_ASSERT(tree.size() == expected.size());
    for (uint64_t index = 0; index < expected.size(); ++index)
    {
        RC_ASSERT(recordsEqual(tree.at(index), expected[index], fieldTypes));
    }
}

void appendAndIterateProperty(const TestUtils::EngineMode mode)
{
    auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS).as("field types");
    if (std::ranges::none_of(fieldTypes, [](const auto type) { return type.type == DataType::Type::VARSIZED; }))
    {
        fieldTypes.back() = DataType{
            DataType::Type::VARSIZED, *rc::gen::arbitrary<bool>() ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE};
    }
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL).as("buffer size");
    RC_PRE(schemaFitsBTreePage(fieldTypes, bufferSize));
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY).as("number of items");
    const auto initialVarSizedMemoryBudget = *rc::gen::inRange<uint64_t>(0, MAX_VARSIZED_MEMORY_BUDGET + 1).as("varsized memory budget");
    auto varSizedMemoryBudget = std::make_shared<uint64_t>(initialVarSizedMemoryBudget);
    auto expected = generateRecords(fieldTypes, numberOfItems, varSizedMemoryBudget);

    NES_INFO(
        "Property BTree appendAndIterate: fields={}, N={}, bufferSize={}, field_types={}",
        fieldTypes.size(),
        expected.size(),
        bufferSize,
        fmt::join(fieldTypes, ", "));

    auto bufferManager
        = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize, initialVarSizedMemoryBudget));
    TestUtils::TestableBTree tree{fieldTypes, *bufferManager, mode};
    for (const auto& record : expected)
    {
        tree.append(record);
    }
    sortOracle(expected, fieldTypes);

    RC_ASSERT(tree.size() == expected.size());
    const auto actual = tree.toVector();
    RC_ASSERT(actual.size() == expected.size());
    for (size_t index = 0; index < expected.size(); ++index)
    {
        RC_ASSERT(recordsEqual(actual[index], expected[index], fieldTypes));
    }
}

void appendAndReadByIndexProperty(const TestUtils::EngineMode mode)
{
    auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS).as("field types");
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL).as("buffer size");
    RC_PRE(schemaFitsBTreePage(fieldTypes, bufferSize));
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY).as("number of items");
    const auto initialVarSizedMemoryBudget = *rc::gen::inRange<uint64_t>(0, MAX_VARSIZED_MEMORY_BUDGET + 1).as("varsized memory budget");
    auto varSizedMemoryBudget = std::make_shared<uint64_t>(initialVarSizedMemoryBudget);
    auto expected = generateRecords(fieldTypes, numberOfItems, varSizedMemoryBudget);

    auto bufferManager
        = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize, initialVarSizedMemoryBudget));
    TestUtils::TestableBTree tree{fieldTypes, *bufferManager, mode};
    for (const auto& record : expected)
    {
        tree.append(record);
    }
    sortOracle(expected, fieldTypes);
    verifyAtExhaustively(tree, expected, fieldTypes);
}

}

RC_GTEST_PROP(BTreePropertyTest, appendAndIterateCompiler, ())
{
    Logger::setupLogging("BTreePropertyTest.log", LogLevel::LOG_DEBUG);
    appendAndIterateProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(BTreePropertyTest, appendAndIterateInterpreter, ())
{
    Logger::setupLogging("BTreePropertyTest.log", LogLevel::LOG_DEBUG);
    appendAndIterateProperty(TestUtils::EngineMode::Interpreter);
}

RC_GTEST_PROP(BTreePropertyTest, appendAndReadByIndexCompiler, ())
{
    Logger::setupLogging("BTreePropertyTest.log", LogLevel::LOG_DEBUG);
    appendAndReadByIndexProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(BTreePropertyTest, appendAndReadByIndexInterpreter, ())
{
    Logger::setupLogging("BTreePropertyTest.log", LogLevel::LOG_DEBUG);
    appendAndReadByIndexProperty(TestUtils::EngineMode::Interpreter);
}

TEST(BTreeTest, DistinctComparatorsInOneCompilation)
{
    constexpr uint64_t bufferSize = 128;
    const std::vector<DataType> fieldTypes{DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}};
    const auto schema = TestUtils::createSchemaFromDataTypes(fieldTypes);
    const auto field = schema[0]->getFullyQualifiedName();
    const std::vector<Record::RecordFieldIdentifier> projections{field};
    auto layout = std::make_shared<DefaultBTreeTupleLayout>(schema);

    const BTreeComparator::RecordComparator ascending = [field](const Record& lhs, const Record& rhs)
    { return (lhs.read(field) < rhs.read(field)).getRawValueAs<nautilus::val<bool>>(); };
    const BTreeComparator::RecordComparator descending = [field](const Record& lhs, const Record& rhs)
    { return (lhs.read(field) > rhs.read(field)).getRawValueAs<nautilus::val<bool>>(); };

    auto bufferManager = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    auto ascendingTree = bufferManager->getUnpooledBuffer(BTree::getMainBufferSize()).value();
    auto descendingTree = bufferManager->getUnpooledBuffer(BTree::getMainBufferSize()).value();
    BTree::init(ascendingTree, bufferSize, schema.getSizeInBytes());
    BTree::init(descendingTree, bufferSize, schema.getSizeInBytes());

    const nautilus::engine::NautilusEngine engine{TestUtils::makeEngine(TestUtils::EngineMode::Compiler)};
    CompilationContext compilationContext{engine.createModule()};
    BTreeComparator ascendingComparator{compilationContext, layout, "ascending", ascending};
    BTreeComparator descendingComparator{compilationContext, layout, "descending", descending};
    auto append = compilationContext.registerFunction(
        std::function(
            [layout, &ascendingComparator, &descendingComparator, projections, fieldTypes](
                nautilus::val<TupleBuffer*> ascendingTree, /// NOLINT(performance-unnecessary-value-param): required by CompilationContext
                nautilus::val<TupleBuffer*> descendingTree, /// NOLINT(performance-unnecessary-value-param): required by CompilationContext
                nautilus::val<AbstractBufferProvider*>
                    provider, /// NOLINT(performance-unnecessary-value-param): required by CompilationContext
                nautilus::val<TestUtils::AnyVec*> values) /// NOLINT(performance-unnecessary-value-param): required by CompilationContext
            {
                const auto record = TestUtils::buildRecordFromAnyVec(values, projections, fieldTypes);
                BTreeRef{BorrowedNautilusBuffer::from(ascendingTree), layout}.append(record, provider, ascendingComparator);
                BTreeRef{BorrowedNautilusBuffer::from(descendingTree), layout}.append(record, provider, descendingComparator);
            }),
        "appendToAscendingAndDescendingBTree");
    compilationContext.compile();

    for (const auto value : std::array<int64_t, 3>{7, -5, 1})
    {
        TestUtils::AnyVec record{value};
        append(&ascendingTree, &descendingTree, bufferManager.get(), &record);
    }

    const auto readValue = [](const TupleBuffer& tree, const uint64_t index)
    {
        int64_t value = 0;
        const auto entry = BTree::load(tree).at(index);
        std::memcpy(&value, entry.data(), sizeof(value));
        return value;
    };
    EXPECT_EQ(readValue(ascendingTree, 0), -5);
    EXPECT_EQ(readValue(ascendingTree, 1), 1);
    EXPECT_EQ(readValue(ascendingTree, 2), 7);
    EXPECT_EQ(readValue(descendingTree, 0), 7);
    EXPECT_EQ(readValue(descendingTree, 1), 1);
    EXPECT_EQ(readValue(descendingTree, 2), -5);
}

}
