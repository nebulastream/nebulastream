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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <tuple>
#include <vector>
#include <Interface/RowHeap/RowHeap.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed by rapidcheck/gtest.h
#include <DataStructureTestUtils.hpp>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

namespace NES
{
namespace
{
/// A row is a UINT64 key, a UINT64 sequence number that makes every row distinct, and the access to a variable-sized value.
/// Rows come first by smaller key, then by smaller sequence number, so the heap keeps the first rows and its root is the last.
constexpr uint64_t KEY_OFFSET = 0;
constexpr uint64_t SEQUENCE_OFFSET = sizeof(uint64_t);
constexpr uint64_t VALUE_OFFSET = 2 * sizeof(uint64_t);
constexpr uint64_t ROW_SIZE = (2 * sizeof(uint64_t)) + sizeof(VariableSizedAccess);

struct Row
{
    uint64_t key;
    uint64_t sequence;
    std::string value;

    friend bool operator<(const Row& lhs, const Row& rhs) { return std::tie(lhs.key, lhs.sequence) < std::tie(rhs.key, rhs.sequence); }
    friend bool operator==(const Row& lhs, const Row& rhs) = default;
};

uint64_t readUInt64(const int8_t* row, const uint64_t offset)
{
    uint64_t value = 0;
    std::memcpy(&value, row + offset, sizeof(value));
    return value;
}

bool comesBefore(const int8_t* first, const int8_t* second)
{
    return std::tuple{readUInt64(first, KEY_OFFSET), readUInt64(first, SEQUENCE_OFFSET)}
    < std::tuple{readUInt64(second, KEY_OFFSET), readUInt64(second, SEQUENCE_OFFSET)};
}

const RowHeap::Comparator rowOrder = comesBefore;

TupleBuffer createHeap(BufferManager& bufferManager, const uint64_t capacity, const uint64_t varSizedPageSize)
{
    auto buffer = bufferManager.getUnpooledBuffer(RowHeap::getMainBufferSize(capacity, ROW_SIZE)).value();
    const std::vector<uint64_t> varSizedFieldOffsets{VALUE_OFFSET};
    RowHeap::init(buffer, bufferManager, capacity, ROW_SIZE, varSizedPageSize, varSizedFieldOffsets);
    return buffer;
}

/// Writes the row as the pending row and adds it if the heap is not full or the row comes before the root.
void insert(BufferManager& bufferManager, const TupleBuffer& buffer, const Row& row)
{
    auto heap = RowHeap::load(buffer);
    auto* pending = heap.clearPending();
    std::memcpy(pending + KEY_OFFSET, &row.key, sizeof(row.key));
    std::memcpy(pending + SEQUENCE_OFFSET, &row.sequence, sizeof(row.sequence));
    const auto access = heap.storeVarSized(bufferManager, std::as_bytes(std::span{row.value}));
    std::memcpy(pending + VALUE_OFFSET, &access, sizeof(access));
    if (heap.size() < heap.capacity())
    {
        heap.pushPending(rowOrder);
    }
    else if (comesBefore(pending, heap.row(0)))
    {
        heap.replacePendingTop(rowOrder);
    }
}

Row readRow(const TupleBuffer& buffer, const uint64_t index)
{
    const auto* row = RowHeap::load(buffer).row(index);
    VariableSizedAccess access;
    std::memcpy(&access, row + VALUE_OFFSET, sizeof(access));
    const auto varSized = buffer.loadChildBuffer(access.getIndex());
    const auto* data = reinterpret_cast<const char*>(varSized.getAvailableMemoryArea<>().data()); /// NOLINT
    return Row{
        .key = readUInt64(row, KEY_OFFSET),
        .sequence = readUInt64(row, SEQUENCE_OFFSET),
        .value = std::string{data + access.getOffset().getRawOffset(), access.getSize().getRawSize()}};
}

std::vector<Row> readRows(const TupleBuffer& buffer)
{
    std::vector<Row> rows;
    for (uint64_t index = 0; index < RowHeap::load(buffer).size(); ++index)
    {
        rows.push_back(readRow(buffer, index));
    }
    return rows;
}

std::string valueFor(const uint64_t key, const uint64_t size)
{
    return fmt::format("{:0>{}}", key, size);
}
}

TEST(RowHeapTest, KeepsTheFirstRowsAndSortsThem)
{
    const auto bufferManager = TestUtils::createBufferManager(4096, 1024);
    const auto buffer = createHeap(*bufferManager, 3, 256);
    std::vector<uint64_t> keys(50);
    std::iota(keys.begin(), keys.end(), 1);
    std::ranges::shuffle(keys, std::mt19937{42});
    for (const auto key : keys)
    {
        insert(*bufferManager, buffer, Row{.key = key, .sequence = 0, .value = valueFor(key, 100)});
    }

    auto heap = RowHeap::load(buffer);
    heap.sortRows(rowOrder);
    ASSERT_EQ(heap.size(), 3);
    for (uint64_t index = 0; index < heap.size(); ++index)
    {
        EXPECT_EQ(readRow(buffer, index), (Row{.key = index + 1, .sequence = 0, .value = valueFor(index + 1, 100)}));
    }
}

TEST(RowHeapTest, CompactionKeepsTheVariableSizedDataBounded)
{
    constexpr uint64_t capacity = 4;
    constexpr uint64_t valueSize = 100;
    constexpr uint64_t pageSize = 256;
    const auto bufferManager = TestUtils::createBufferManager(4096, 1024);
    const auto buffer = createHeap(*bufferManager, capacity, pageSize);
    /// Descending keys: every new row replaces the root, so every insert turns a value into garbage.
    const auto oldVarSized = buffer.loadChildBuffer(ChildBufferIndex{0});
    for (uint64_t key = 1000; key > 0; --key)
    {
        insert(*bufferManager, buffer, Row{.key = key, .sequence = 0, .value = valueFor(key, valueSize)});
        const auto heap = RowHeap::load(buffer);
        EXPECT_LE(heap.getVarSizedBytesUsed(), heap.getVarSizedCapacity());
        EXPECT_LE(heap.getVarSizedCapacity(), std::max(pageSize, 2 * ((capacity + 1) * valueSize)));
    }
    /// Compaction replaced the first variable-sized buffer instead of adding children, so only this test still holds it.
    EXPECT_EQ(buffer.getNumberOfChildBuffers(), 1);
    EXPECT_EQ(oldVarSized.getReferenceCounter(), 1);
}

/// Inserts random rows, with repeated keys and values of any size including empty ones, into heaps of up to 1024 rows. After every
/// insert, it checks the size, the heap order and the bound on the variable-sized data; every 64 inserts and at the end, it compares
/// the rows and their values with the first rows of all rows so far.
RC_GTEST_PROP(RowHeapPropertyTest, keepsTheFirstRowsWithTheirValues, ())
{
    /// Small heaps fill and replace their rows early, large ones exercise deep sifts and large compactions.
    const auto capacity = *rc::gen::oneOf(rc::gen::inRange<uint64_t>(1, 17), rc::gen::inRange<uint64_t>(1, 1025)).as("capacity");
    const auto pageSize = *rc::gen::element<uint64_t>(16, 256, 4096).as("variable-sized page size");
    const auto maxValueSize = *rc::gen::inRange<uint64_t>(0, 300).as("maximal value size");
    const auto numberOfRows = *rc::gen::inRange<uint64_t>(0, 3000).as("number of rows");
    const auto maxKey = *rc::gen::inRange<uint64_t>(1, 2000).as("maximal key");
    const auto bufferManager = TestUtils::createBufferManager(4096, 4096);
    const auto buffer = createHeap(*bufferManager, capacity, pageSize);

    /// The first rows so far: an ordered set that drops its last row once it holds more than capacity rows.
    std::vector<Row> expected;
    const auto compareWithExpected = [&]
    {
        auto actual = readRows(buffer);
        std::sort(actual.begin(), actual.end());
        RC_ASSERT(actual == expected);
    };
    for (uint64_t sequence = 0; sequence < numberOfRows; ++sequence)
    {
        const auto key = *rc::gen::inRange<uint64_t>(0, maxKey);
        const auto valueSize = *rc::gen::inRange<uint64_t>(0, maxValueSize + 1);
        const Row row{.key = key, .sequence = sequence, .value = valueFor(key, valueSize).substr(0, valueSize)};
        insert(*bufferManager, buffer, row);
        expected.insert(std::upper_bound(expected.begin(), expected.end(), row), row);
        if (expected.size() > capacity)
        {
            expected.pop_back();
        }

        const auto heap = RowHeap::load(buffer);
        RC_ASSERT(heap.size() == expected.size());
        for (uint64_t index = 1; index < heap.size(); ++index)
        {
            RC_ASSERT(not comesBefore(heap.row((index - 1) / 2), heap.row(index)));
        }
        RC_ASSERT(heap.getVarSizedBytesUsed() <= heap.getVarSizedCapacity());
        RC_ASSERT(heap.getVarSizedCapacity() <= std::max(pageSize, 2 * ((capacity + 2) * maxValueSize)));
        RC_ASSERT(buffer.getNumberOfChildBuffers() == 1);
        if (sequence % 64 == 0)
        {
            compareWithExpected();
        }
    }
    compareWithExpected();

    auto heap = RowHeap::load(buffer);
    heap.sortRows(rowOrder);
    RC_ASSERT(readRows(buffer) == expected);
}

}
