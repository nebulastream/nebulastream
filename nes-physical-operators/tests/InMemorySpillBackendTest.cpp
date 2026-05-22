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

}
