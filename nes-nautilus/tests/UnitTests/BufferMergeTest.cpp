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

#include <cstddef>
#include <cstdint>
#include <exception> /// NOLINT(misc-include-cleaner): rapidcheck uses std::exception_ptr without including it
#include <memory>
#include <string>
#include <vector>

#include <rapidcheck.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/BufferMerge.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed via macros expanded from rapidcheck/gtest.h
#include <rapidcheck/gtest.h>
#include <BaseUnitTest.hpp>
#include <DataStructureTestUtils.hpp>
#include <ErrorHandling.hpp>
#include <TestTupleBuffer.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
constexpr uint64_t BUFFER_SIZE = 1024;
constexpr uint64_t NUMBER_OF_BUFFERS = 1024;
constexpr size_t MAX_FIELDS = 6;

/// Writes `record` behind the tuples of `buffer` through `ref`.
void appendRecord(
    const TupleBufferRef& ref,
    TupleBuffer& buffer,
    AbstractBufferProvider& bufferProvider,
    TestUtils::AnyVec& record,
    const std::vector<Record::RecordFieldIdentifier>& names,
    const std::vector<DataType>& types)
{
    const RecordBuffer recordBuffer{BorrowedNautilusBuffer::from(nautilus::val<TupleBuffer*>(std::addressof(buffer)))};
    nautilus::val<uint64_t> index(buffer.getNumberOfTuples());
    ref.writeRecord(
        index,
        recordBuffer,
        TestUtils::buildRecordFromAnyVec(nautilus::val<TestUtils::AnyVec*>(std::addressof(record)), names, types),
        nautilus::val<AbstractBufferProvider*>(std::addressof(bufferProvider)));
    buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
}

TestUtils::AnyVec readRecord(
    const TupleBufferRef& ref,
    TupleBuffer& buffer,
    const uint64_t index,
    const std::vector<Record::RecordFieldIdentifier>& names,
    const std::vector<DataType>& types)
{
    const RecordBuffer recordBuffer{BorrowedNautilusBuffer::from(nautilus::val<TupleBuffer*>(std::addressof(buffer)))};
    nautilus::val<uint64_t> recordIndex(index);
    TestUtils::AnyVec record(types.size());
    TestUtils::storeRecordToAnyVec(
        nautilus::val<TestUtils::AnyVec*>(std::addressof(record)), ref.readRecord(names, recordBuffer, recordIndex), names, types);
    return record;
}

BufferLayout layoutOf(const Testing::TestSchema& schema, const MemoryLayoutType layoutType, const uint64_t bufferSize)
{
    const auto layout = LowerSchemaProvider::lowerSchema(bufferSize, schema, layoutType)->getBufferLayout();
    INVARIANT(layout.has_value(), "Row and columnar layouts describe themselves");
    return *layout;
}

BufferLayout layoutOf(const Testing::TestSchema& schema, const MemoryLayoutType layoutType)
{
    return layoutOf(schema, layoutType, BUFFER_SIZE);
}

Testing::TestSchema keyAndPayload()
{
    return Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("key"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("payload"), DataType::Type::VARSIZED}};
}

/// A buffer with one record per payload, keyed by its position in `payloads`.
TupleBuffer withPayloads(BufferManager& bufferManager, const std::vector<std::string>& payloads, const MemoryLayoutType layoutType)
{
    auto buffer = bufferManager.getBufferBlocking();
    auto view = Testing::TestTupleBuffer{keyAndPayload(), layoutType}.open(buffer, &bufferManager);
    for (uint64_t key = 0; key < payloads.size(); ++key)
    {
        view.append(key, payloads[key]);
    }
    return buffer;
}

std::vector<std::string> payloadsIn(TupleBuffer& buffer, BufferManager& bufferManager, const MemoryLayoutType layoutType)
{
    auto view = Testing::TestTupleBuffer{keyAndPayload(), layoutType}.open(buffer, &bufferManager);
    std::vector<std::string> payloads;
    payloads.reserve(view.getNumberOfTuples());
    for (size_t i = 0; i < view.getNumberOfTuples(); ++i)
    {
        payloads.push_back(view[i]["payload"].as<std::string>());
    }
    return payloads;
}
}

class BufferMergeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("BufferMergeTest.log", LogLevel::LOG_DEBUG); }

    std::shared_ptr<BufferManager> bufferManager = TestUtils::createBufferManager(BUFFER_SIZE, NUMBER_OF_BUFFERS);
};

/// Records written through a row or columnar layout, split across buffers and merged back, read the same as before.
RC_GTEST_PROP(BufferMergePropertyTest, MergedBuffersKeepEveryRecord, ())
{
    const auto types = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, MAX_FIELDS);
    const auto layoutType = *rc::gen::element(MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::COLUMNAR_LAYOUT);
    const auto ref = LowerSchemaProvider::lowerSchema(BUFFER_SIZE, TestUtils::createSchemaFromDataTypes(types), layoutType);
    const auto names = ref->getAllFieldNames();
    const auto describedLayout = ref->getBufferLayout();
    INVARIANT(describedLayout.has_value(), "Row and columnar layouts describe themselves");
    const auto& layout = *describedLayout;
    RC_PRE(layout.capacity > 0);
    const auto sourceSizes = *rc::gen::container<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(0, layout.capacity + 1));

    const auto bufferManager = TestUtils::createBufferManager(BUFFER_SIZE, NUMBER_OF_BUFFERS);
    std::vector<TestUtils::AnyVec> reference;
    std::vector<TupleBuffer> merged{bufferManager->getBufferBlocking()};
    for (const auto size : sourceSizes)
    {
        auto source = bufferManager->getBufferBlocking();
        for (uint64_t i = 0; i < size; ++i)
        {
            reference.push_back(*TestUtils::genAnyVec(types));
            appendRecord(*ref, source, *bufferManager, reference.back(), names, types);
        }
        if (merged.back().getNumberOfTuples() + size > layout.capacity)
        {
            merged.push_back(bufferManager->getBufferBlocking());
        }
        appendTuples(merged.back(), source, layout);
    }

    size_t next = 0;
    for (auto& buffer : merged)
    {
        for (uint64_t i = 0; i < buffer.getNumberOfTuples(); ++i)
        {
            RC_ASSERT(next < reference.size());
            RC_ASSERT(TestUtils::anyVecsEqual(readRecord(*ref, buffer, i, names, types), reference[next], types));
            ++next;
        }
    }
    RC_ASSERT(next == reference.size());
}

TEST_F(BufferMergeTest, PayloadsOfSingleChildSourcesShareTheLastChild)
{
    for (const auto layoutType : {MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::COLUMNAR_LAYOUT})
    {
        const auto layout = layoutOf(keyAndPayload(), layoutType);
        auto target = bufferManager->getBufferBlocking();
        appendTuples(target, withPayloads(*bufferManager, {"abcd", "ef"}, layoutType), layout);
        appendTuples(target, withPayloads(*bufferManager, {"wxyz"}, layoutType), layout);
        appendTuples(target, withPayloads(*bufferManager, {"12"}, layoutType), layout);

        EXPECT_EQ(target.getNumberOfChildBuffers(), 1);
        EXPECT_EQ(payloadsIn(target, *bufferManager, layoutType), (std::vector<std::string>{"abcd", "ef", "wxyz", "12"}));
    }
}

/// A payload that does not fit behind the last child is adopted with its child, and later payloads are copied behind that one.
TEST_F(BufferMergeTest, PayloadThatDoesNotFitAdoptsItsChild)
{
    const auto layout = layoutOf(keyAndPayload(), MemoryLayoutType::ROW_LAYOUT);
    const std::string large(BUFFER_SIZE - 2, 'a');
    auto target = withPayloads(*bufferManager, {large}, MemoryLayoutType::ROW_LAYOUT);
    appendTuples(target, withPayloads(*bufferManager, {"wxyz"}, MemoryLayoutType::ROW_LAYOUT), layout);
    appendTuples(target, withPayloads(*bufferManager, {"12"}, MemoryLayoutType::ROW_LAYOUT), layout);

    EXPECT_EQ(target.getNumberOfChildBuffers(), 2);
    EXPECT_EQ(payloadsIn(target, *bufferManager, MemoryLayoutType::ROW_LAYOUT), (std::vector<std::string>{large, "wxyz", "12"}));
}

/// Sources whose payloads span several children are adopted, and their references point to the adopted children.
TEST_F(BufferMergeTest, ReferencesAreRebasedOntoAdoptedChildren)
{
    for (const auto layoutType : {MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::COLUMNAR_LAYOUT})
    {
        const auto layout = layoutOf(keyAndPayload(), layoutType);
        const std::string first((BUFFER_SIZE / 2) + 1, 'x');
        const std::string second((BUFFER_SIZE / 2) + 1, 'y');
        auto target = withPayloads(*bufferManager, {"ab"}, layoutType);
        appendTuples(target, withPayloads(*bufferManager, {second, first}, layoutType), layout);

        EXPECT_EQ(target.getNumberOfChildBuffers(), 3);
        EXPECT_EQ(payloadsIn(target, *bufferManager, layoutType), (std::vector<std::string>{"ab", second, first}));
    }
}

TEST_F(BufferMergeTest, AcceptsBuffersLargerThanTheLayout)
{
    const auto layout = layoutOf(keyAndPayload(), MemoryLayoutType::ROW_LAYOUT, BUFFER_SIZE / 2);
    auto target = bufferManager->getBufferBlocking();
    appendTuples(target, withPayloads(*bufferManager, {"abcd"}, MemoryLayoutType::ROW_LAYOUT), layout);
    EXPECT_EQ(payloadsIn(target, *bufferManager, MemoryLayoutType::ROW_LAYOUT), (std::vector<std::string>{"abcd"}));
}

TEST(BufferMergeDeathTest, RejectsTuplesThatDoNotFit)
{
    const auto bufferManager = TestUtils::createBufferManager(BUFFER_SIZE, NUMBER_OF_BUFFERS);
    const auto layout = layoutOf(keyAndPayload(), MemoryLayoutType::ROW_LAYOUT);
    auto target = bufferManager->getBufferBlocking();
    target.setNumberOfTuples(layout.capacity);
    const auto source = withPayloads(*bufferManager, {"abcd"}, MemoryLayoutType::ROW_LAYOUT);
    EXPECT_DEATH_DEBUG(appendTuples(target, source, layout), "");
}

TEST(BufferMergeDeathTest, RejectsBuffersSmallerThanTheLayout)
{
    const auto bufferManager = TestUtils::createBufferManager(BUFFER_SIZE, NUMBER_OF_BUFFERS);
    const auto layout = layoutOf(keyAndPayload(), MemoryLayoutType::ROW_LAYOUT);
    auto target = bufferManager->getUnpooledBuffer(BUFFER_SIZE / 2);
    ASSERT_TRUE(target.has_value());
    const auto source = withPayloads(*bufferManager, {"abcd"}, MemoryLayoutType::ROW_LAYOUT);
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) checked by the assertion above
    EXPECT_DEATH_DEBUG(appendTuples(*target, source, layout), "");
}

}
