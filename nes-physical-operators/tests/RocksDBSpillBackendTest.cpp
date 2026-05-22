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
#include <filesystem>
#include <initializer_list>
#include <string>

#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SliceStore/RocksDBSpillBackend.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>

namespace NES::Testing
{

class RocksDBSpillBackendTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("RocksDBSpillBackendTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup RocksDBSpillBackendTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        const auto* const info = ::testing::UnitTest::GetInstance()->current_test_info();
        dbPath = std::filesystem::temp_directory_path()
            / (std::string("nes-rocksdb-") + info->test_suite_name() + "-" + info->name());
        std::filesystem::remove_all(dbPath); /// start from a clean directory
    }

    void TearDown() override
    {
        std::error_code ec;
        std::filesystem::remove_all(dbPath, ec);
        BaseUnitTest::TearDown();
    }

protected:
    std::filesystem::path dbPath;

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

TEST_F(RocksDBSpillBackendTest, putThenGetReturnsStoredRecord)
{
    RocksDBSpillBackend backend(dbPath.string());
    const SliceEnd sliceEnd{Timestamp(10)};
    const WorkerThreadId workerThreadId{2};
    const SpillRecord record = makeRecord({1, 2, 3, 4, 5});

    backend.put(sliceEnd, workerThreadId, record);

    const auto fetched = backend.get(sliceEnd, workerThreadId);
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, record);
}

TEST_F(RocksDBSpillBackendTest, getMissesForUnknownKeys)
{
    RocksDBSpillBackend backend(dbPath.string());
    backend.put(SliceEnd{Timestamp(10)}, WorkerThreadId{0}, makeRecord({9}));

    EXPECT_FALSE(backend.get(SliceEnd{Timestamp(11)}, WorkerThreadId{0}).has_value());
    EXPECT_FALSE(backend.get(SliceEnd{Timestamp(10)}, WorkerThreadId{1}).has_value());
}

TEST_F(RocksDBSpillBackendTest, eraseDropsAllWorkersOfSliceOnly)
{
    RocksDBSpillBackend backend(dbPath.string());
    const SliceEnd target{Timestamp(10)};
    const SliceEnd other{Timestamp(20)};
    backend.put(target, WorkerThreadId{0}, makeRecord({1}));
    backend.put(target, WorkerThreadId{1}, makeRecord({2}));
    backend.put(other, WorkerThreadId{0}, makeRecord({3}));

    backend.erase(target);

    EXPECT_FALSE(backend.get(target, WorkerThreadId{0}).has_value());
    EXPECT_FALSE(backend.get(target, WorkerThreadId{1}).has_value());
    EXPECT_TRUE(backend.get(other, WorkerThreadId{0}).has_value()); /// untouched
}

TEST_F(RocksDBSpillBackendTest, putOverwritesExistingRecord)
{
    RocksDBSpillBackend backend(dbPath.string());
    const SliceEnd sliceEnd{Timestamp(10)};
    const WorkerThreadId workerThreadId{0};

    backend.put(sliceEnd, workerThreadId, makeRecord({1, 1}));
    backend.put(sliceEnd, workerThreadId, makeRecord({2, 2, 2}));

    const auto fetched = backend.get(sliceEnd, workerThreadId);
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, makeRecord({2, 2, 2}));
}

/// The defining property over the in-memory backend: records survive closing and reopening the DB.
TEST_F(RocksDBSpillBackendTest, recordsSurviveReopen)
{
    const SliceEnd sliceEnd{Timestamp(42)};
    const WorkerThreadId workerThreadId{3};
    const SpillRecord record = makeRecord({7, 7, 7, 7});

    {
        RocksDBSpillBackend backend(dbPath.string());
        backend.put(sliceEnd, workerThreadId, record);
    } /// backend destroyed -> DB closed

    RocksDBSpillBackend reopened(dbPath.string());
    const auto fetched = reopened.get(sliceEnd, workerThreadId);
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, record);
}

TEST_F(RocksDBSpillBackendTest, emptyRecordRoundTrips)
{
    RocksDBSpillBackend backend(dbPath.string());
    const SliceEnd sliceEnd{Timestamp(1)};
    const WorkerThreadId workerThreadId{0};

    backend.put(sliceEnd, workerThreadId, SpillRecord{});

    const auto fetched = backend.get(sliceEnd, workerThreadId);
    ASSERT_TRUE(fetched.has_value());
    EXPECT_TRUE(fetched->empty());
}

TEST_F(RocksDBSpillBackendTest, eraseOfUnknownSliceIsNoOp)
{
    RocksDBSpillBackend backend(dbPath.string());
    backend.put(SliceEnd{Timestamp(10)}, WorkerThreadId{0}, makeRecord({1}));

    EXPECT_NO_THROW(backend.erase(SliceEnd{Timestamp(999)}));
    EXPECT_TRUE(backend.get(SliceEnd{Timestamp(10)}, WorkerThreadId{0}).has_value()); /// existing record untouched
}

TEST_F(RocksDBSpillBackendTest, rejectsUnknownCompression)
{
    const auto badPath = (dbPath.string() + "-bad");
    ASSERT_EXCEPTION_ERRORCODE(RocksDBSpillBackend(badPath, "bogus"), ErrorCode::SpillStoreFailure);
}

}
