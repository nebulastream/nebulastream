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
#include <span>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SliceStore/InMemorySpillBackend.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>

namespace NES::Testing
{

class InMemorySpillBackendTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("InMemorySpillBackendTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup InMemorySpillBackendTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

protected:
    static SpillRecord makeRecord(const std::initializer_list<unsigned char> bytes)
    {
        SpillRecord record;
        for (const auto byte : bytes)
        {
            record.push_back(static_cast<std::byte>(byte));
        }
        return record;
    }
};

TEST_F(InMemorySpillBackendTest, putThenGetReturnsStoredRecord)
{
    InMemorySpillBackend backend;
    const SliceEnd sliceEnd{Timestamp(10)};
    const WorkerThreadId workerThreadId{2};
    const SpillRecord record = makeRecord({1, 2, 3, 4});

    backend.put(sliceEnd, workerThreadId, record);

    const auto fetched = backend.get(sliceEnd, workerThreadId);
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, record);
}

TEST_F(InMemorySpillBackendTest, getMissesForUnknownKeys)
{
    InMemorySpillBackend backend;
    backend.put(SliceEnd{Timestamp(10)}, WorkerThreadId{0}, makeRecord({9}));

    EXPECT_FALSE(backend.get(SliceEnd{Timestamp(11)}, WorkerThreadId{0}).has_value()); /// other slice
    EXPECT_FALSE(backend.get(SliceEnd{Timestamp(10)}, WorkerThreadId{1}).has_value()); /// other worker
}

TEST_F(InMemorySpillBackendTest, eraseDropsAllWorkersOfSliceOnly)
{
    InMemorySpillBackend backend;
    const SliceEnd target{Timestamp(10)};
    const SliceEnd other{Timestamp(20)};
    backend.put(target, WorkerThreadId{0}, makeRecord({1}));
    backend.put(target, WorkerThreadId{1}, makeRecord({2}));
    backend.put(other, WorkerThreadId{0}, makeRecord({3}));
    ASSERT_EQ(backend.sliceCount(), 2U);

    backend.erase(target);

    EXPECT_FALSE(backend.get(target, WorkerThreadId{0}).has_value());
    EXPECT_FALSE(backend.get(target, WorkerThreadId{1}).has_value());
    EXPECT_TRUE(backend.get(other, WorkerThreadId{0}).has_value()); /// untouched
    EXPECT_EQ(backend.sliceCount(), 1U);
}

TEST_F(InMemorySpillBackendTest, putOverwritesExistingRecord)
{
    InMemorySpillBackend backend;
    const SliceEnd sliceEnd{Timestamp(10)};
    const WorkerThreadId workerThreadId{0};

    backend.put(sliceEnd, workerThreadId, makeRecord({1, 1}));
    backend.put(sliceEnd, workerThreadId, makeRecord({2, 2, 2}));

    const auto fetched = backend.get(sliceEnd, workerThreadId);
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, makeRecord({2, 2, 2}));
}

TEST_F(InMemorySpillBackendTest, eraseOfUnknownSliceIsNoOp)
{
    InMemorySpillBackend backend;
    backend.put(SliceEnd{Timestamp(10)}, WorkerThreadId{0}, makeRecord({1}));

    EXPECT_NO_THROW(backend.erase(SliceEnd{Timestamp(999)}));
    EXPECT_EQ(backend.sliceCount(), 1U);
}

/// Phase 2 (2b): distinct partitions of the same (sliceEnd, worker) round-trip independently,
/// and an unwritten partition misses.
TEST_F(InMemorySpillBackendTest, perPartitionRoundTrip)
{
    InMemorySpillBackend backend;
    const SliceEnd sliceEnd{Timestamp(10)};
    const WorkerThreadId workerThreadId{2};
    const SpillRecord recordA = makeRecord({1, 2, 3});
    const SpillRecord recordB = makeRecord({4, 5, 6, 7});

    backend.put(sliceEnd, workerThreadId, recordA, 0);
    backend.put(sliceEnd, workerThreadId, recordB, 1);

    const auto fetchedA = backend.get(sliceEnd, workerThreadId, 0);
    const auto fetchedB = backend.get(sliceEnd, workerThreadId, 1);
    ASSERT_TRUE(fetchedA.has_value());
    ASSERT_TRUE(fetchedB.has_value());
    EXPECT_EQ(*fetchedA, recordA);
    EXPECT_EQ(*fetchedB, recordB);

    EXPECT_FALSE(backend.get(sliceEnd, workerThreadId, 2).has_value()); /// unwritten partition
}

/// Phase 2 (2b): erase(SliceEnd) drops every (worker, partition) record of the slice.
TEST_F(InMemorySpillBackendTest, eraseDropsAllPartitions)
{
    InMemorySpillBackend backend;
    const SliceEnd sliceEnd{Timestamp(10)};
    const WorkerThreadId workerThreadId{0};
    backend.put(sliceEnd, workerThreadId, makeRecord({1}), 0);
    backend.put(sliceEnd, workerThreadId, makeRecord({2}), 1);
    backend.put(sliceEnd, workerThreadId, makeRecord({3}), 2);

    backend.erase(sliceEnd);

    EXPECT_FALSE(backend.get(sliceEnd, workerThreadId, 0).has_value());
    EXPECT_FALSE(backend.get(sliceEnd, workerThreadId, 1).has_value());
    EXPECT_FALSE(backend.get(sliceEnd, workerThreadId, 2).has_value());
    EXPECT_EQ(backend.sliceCount(), 0U);
}

/// Phase 2 (2b): erasing one slice leaves another slice's partitions intact (prefix-sweep stays bounded).
TEST_F(InMemorySpillBackendTest, eraseStaysBoundedToOneSlice)
{
    InMemorySpillBackend backend;
    const SliceEnd sliceA{Timestamp(10)};
    const SliceEnd sliceB{Timestamp(20)};
    const WorkerThreadId workerThreadId{0};
    backend.put(sliceA, workerThreadId, makeRecord({1}), 0);
    backend.put(sliceA, workerThreadId, makeRecord({2}), 1);
    backend.put(sliceB, workerThreadId, makeRecord({3}), 0);
    backend.put(sliceB, workerThreadId, makeRecord({4}), 1);

    backend.erase(sliceA);

    EXPECT_FALSE(backend.get(sliceA, workerThreadId, 0).has_value());
    EXPECT_FALSE(backend.get(sliceA, workerThreadId, 1).has_value());
    EXPECT_TRUE(backend.get(sliceB, workerThreadId, 0).has_value()); /// untouched
    EXPECT_TRUE(backend.get(sliceB, workerThreadId, 1).has_value()); /// untouched
}

/// Phase 2 (2b): put/get without the partition arg behaves exactly as partition 0.
TEST_F(InMemorySpillBackendTest, defaultPartitionEqualsZero)
{
    InMemorySpillBackend backend;
    const SliceEnd sliceEnd{Timestamp(10)};
    const WorkerThreadId workerThreadId{1};
    const SpillRecord record = makeRecord({8, 8});

    backend.put(sliceEnd, workerThreadId, record); /// no partition arg -> p == 0

    const auto fetched = backend.get(sliceEnd, workerThreadId, 0); /// explicit p == 0
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, record);
    EXPECT_TRUE(backend.contains(sliceEnd, workerThreadId)); /// contains default p == 0
}

/// Phase 2 (2b): (worker=0, p=1) and (worker=1, p=0) are fully distinct keys with independent
/// records — guards against worker/partition byte-order confusion in the key.
TEST_F(InMemorySpillBackendTest, workerPartitionKeyIsFullyDistinct)
{
    InMemorySpillBackend backend;
    const SliceEnd sliceEnd{Timestamp(10)};
    backend.put(sliceEnd, WorkerThreadId{0}, makeRecord({0xA}), 1);
    backend.put(sliceEnd, WorkerThreadId{1}, makeRecord({0xB}), 0);

    const auto fetchedW0P1 = backend.get(sliceEnd, WorkerThreadId{0}, 1);
    const auto fetchedW1P0 = backend.get(sliceEnd, WorkerThreadId{1}, 0);
    ASSERT_TRUE(fetchedW0P1.has_value());
    ASSERT_TRUE(fetchedW1P0.has_value());
    EXPECT_EQ(*fetchedW0P1, makeRecord({0xA}));
    EXPECT_EQ(*fetchedW1P0, makeRecord({0xB}));

    EXPECT_FALSE(backend.get(sliceEnd, WorkerThreadId{0}, 0).has_value()); /// wrong partition for worker 0
    EXPECT_FALSE(backend.get(sliceEnd, WorkerThreadId{1}, 1).has_value()); /// wrong partition for worker 1
}

}
