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
#include <limits>
#include <memory>
#include <string>

#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SliceStore/InMemorySpillBackend.hpp>
#include <SliceStore/NullSpillBackend.hpp>
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

/// Phase 2 (2b): distinct partitions of the same (sliceEnd, worker) round-trip independently,
/// and an unwritten partition misses.
TEST_F(RocksDBSpillBackendTest, perPartitionRoundTrip)
{
    RocksDBSpillBackend backend(dbPath.string());
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

/// Phase 2 (2b): erase(SliceEnd) drops every (worker, partition) record of the slice
/// (the 8-byte slice prefix sweep collects all partition suffixes).
TEST_F(RocksDBSpillBackendTest, eraseDropsAllPartitions)
{
    RocksDBSpillBackend backend(dbPath.string());
    const SliceEnd sliceEnd{Timestamp(10)};
    const WorkerThreadId workerThreadId{0};
    backend.put(sliceEnd, workerThreadId, makeRecord({1}), 0);
    backend.put(sliceEnd, workerThreadId, makeRecord({2}), 1);
    backend.put(sliceEnd, workerThreadId, makeRecord({3}), 2);

    backend.erase(sliceEnd);

    EXPECT_FALSE(backend.get(sliceEnd, workerThreadId, 0).has_value());
    EXPECT_FALSE(backend.get(sliceEnd, workerThreadId, 1).has_value());
    EXPECT_FALSE(backend.get(sliceEnd, workerThreadId, 2).has_value());
}

/// Phase 2 (2b): erasing one slice leaves another slice's partitions intact — the prefix sweep
/// stays bounded to the 8-byte sliceEnd prefix even with the 2-byte partition suffix present.
TEST_F(RocksDBSpillBackendTest, eraseStaysBoundedToOneSlice)
{
    RocksDBSpillBackend backend(dbPath.string());
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
TEST_F(RocksDBSpillBackendTest, defaultPartitionEqualsZero)
{
    RocksDBSpillBackend backend(dbPath.string());
    const SliceEnd sliceEnd{Timestamp(10)};
    const WorkerThreadId workerThreadId{1};
    const SpillRecord record = makeRecord({8, 8});

    backend.put(sliceEnd, workerThreadId, record); /// no partition arg -> p == 0

    const auto fetched = backend.get(sliceEnd, workerThreadId, 0); /// explicit p == 0
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, record);
}

/// Phase 2 (2b): (worker=0, p=1) and (worker=1, p=0) are fully distinct keys with independent
/// records — guards against worker/partition byte-order confusion in the 14-byte key encoding.
TEST_F(RocksDBSpillBackendTest, workerPartitionKeyIsFullyDistinct)
{
    RocksDBSpillBackend backend(dbPath.string());
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

/// Phase 2 (2b): a PartitionId at uint16_t max exercises the 2-byte big-endian suffix at its
/// boundary — confirms no truncation/sign-extension in the key encoding, and a different high
/// partition still misses.
TEST_F(RocksDBSpillBackendTest, highPartitionIdRoundTrips)
{
    RocksDBSpillBackend backend(dbPath.string());
    const SliceEnd sliceEnd{Timestamp(1)};
    const WorkerThreadId workerThreadId{0};
    constexpr PartitionId maxPartition = std::numeric_limits<PartitionId>::max(); /// 65535
    const SpillRecord record = makeRecord({0xFF, 0xFE});

    backend.put(sliceEnd, workerThreadId, record, maxPartition);

    const auto fetched = backend.get(sliceEnd, workerThreadId, maxPartition);
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, record);
    EXPECT_FALSE(backend.get(sliceEnd, workerThreadId, maxPartition - 1).has_value()); /// other high partition misses
}

/// Phase 0a: explicit write_buffer_size + block_cache (bytes) are accepted and do not alter correctness —
/// records still round-trip durably across reopen. (The options bound RocksDB's memory tax; behavior is unchanged.)
TEST_F(RocksDBSpillBackendTest, honorsExplicitMemoryBudgetAndRoundTrips)
{
    constexpr std::size_t writeBufferBytes = 4ULL * 1024 * 1024; /// 4 MiB
    constexpr std::size_t blockCacheBytes = 8ULL * 1024 * 1024; /// 8 MiB
    const SliceEnd sliceEnd{Timestamp(7)};
    const WorkerThreadId workerThreadId{1};
    const SpillRecord record = makeRecord({4, 2, 4, 2});

    {
        RocksDBSpillBackend backend(dbPath.string(), "lz4", writeBufferBytes, blockCacheBytes);
        backend.put(sliceEnd, workerThreadId, record);
        const auto fetched = backend.get(sliceEnd, workerThreadId);
        ASSERT_TRUE(fetched.has_value());
        EXPECT_EQ(*fetched, record);
    } /// closed

    /// Reopen with the DEFAULT (0,0) budget: persisted data must be independent of the memory-budget options.
    RocksDBSpillBackend reopened(dbPath.string());
    const auto fetched = reopened.get(sliceEnd, workerThreadId);
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, record);
}

/// Zero budgets (the default) leave RocksDB's own defaults in place and behave exactly like the 2-arg ctor.
TEST_F(RocksDBSpillBackendTest, zeroMemoryBudgetIsBehaviorPreserving)
{
    RocksDBSpillBackend backend(dbPath.string(), "lz4", 0, 0);
    const SliceEnd sliceEnd{Timestamp(3)};
    const WorkerThreadId workerThreadId{0};
    backend.put(sliceEnd, workerThreadId, makeRecord({1, 1, 1}));

    const auto fetched = backend.get(sliceEnd, workerThreadId);
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(*fetched, makeRecord({1, 1, 1}));
}

// ============================================================================
// E1-PR2: BackendStats / getBackendStats() tests
// ----------------------------------------------------------------------------
// RED on baseline (before this PR): getBackendStats() does not exist on
// SpillBackend, so all three tests below fail to compile / link.
// GREEN after this PR: all assertions pass.
// ============================================================================

/// E1-PR2 (a): RocksDBSpillBackend returns a BackendStats after writes.
///
/// Why RED on baseline: SpillBackend::getBackendStats() does not exist before
/// this PR; the call fails at compile time.
///
/// Puts enough records to reliably increase bytesWritten; does NOT force a
/// flush (that is OS/RocksDB-schedule dependent), so sstFootprintBytes and
/// bytesFlushed may be 0 in a short run.  The test therefore only asserts
/// the invariants that are guaranteed after any write:
///   - getBackendStats() returns a value (not nullopt).
///   - sstFootprintBytes >= 0 (the property type is unsigned — always true).
///   - writeAmplification >= 0.0 (defined as 0.0 when bytesFlushed == 0).
///   - bytesWritten increases proportionally with the number of puts.
///
/// We do NOT assert bytesWritten == exact_byte_count because RocksDB counts
/// the uncompressed key+value bytes through its WAL path, which may include
/// metadata overhead; we only assert the counter is positive after writes.
TEST_F(RocksDBSpillBackendTest, getBackendStatsReturnsValueAfterWrites)
{
    RocksDBSpillBackend backend(dbPath.string());

    /// Write several records to ensure BYTES_WRITTEN ticker advances.
    constexpr int NUM_RECORDS = 20;
    for (int i = 0; i < NUM_RECORDS; ++i)
    {
        const SliceEnd sliceEnd{Timestamp(static_cast<uint64_t>(i + 1))};
        backend.put(sliceEnd, WorkerThreadId{0}, makeRecord({0xAA, 0xBB, 0xCC, 0xDD}));
    }

    const auto stats = backend.getBackendStats();
    ASSERT_TRUE(stats.has_value()) << "RocksDBSpillBackend must return a BackendStats";

    /// Non-negative invariants (unsigned fields are always >= 0; double check the double).
    EXPECT_GE(stats->writeAmplification, 0.0);

    /// bytesWritten must be > 0 after writes: RocksDB always accounts user bytes.
    EXPECT_GT(stats->bytesWritten, 0U) << "bytesWritten should be positive after puts";

    /// sstFootprintBytes == 0 is acceptable before a flush; it must not underflow.
    EXPECT_GE(stats->sstFootprintBytes, 0U);

    /// If a flush happened (bytesFlushed > 0), writeAmplification must be >= 1.0
    /// (you can never write fewer bytes to SSTs than you flush from memtable).
    if (stats->bytesFlushed > 0)
    {
        EXPECT_GE(stats->writeAmplification, 1.0)
            << "writeAmplification must be >= 1.0 when flushes occurred";
        EXPECT_GT(stats->sstFootprintBytes, 0U)
            << "sstFootprintBytes must be positive after at least one flush";
    }
}

/// E1-PR2 (b): bytesWritten increases monotonically as more puts are issued.
///
/// Opens one backend, issues a first batch, checks bytesWritten, issues a
/// second batch, and asserts the counter grew.  This exercises the live
/// ticker accumulation model (statistics are not reset between puts).
TEST_F(RocksDBSpillBackendTest, bytesWrittenIncreasesWithMorePuts)
{
    RocksDBSpillBackend backend(dbPath.string());

    /// First batch.
    backend.put(SliceEnd{Timestamp(1)}, WorkerThreadId{0}, makeRecord({1, 2, 3, 4}));
    backend.put(SliceEnd{Timestamp(2)}, WorkerThreadId{0}, makeRecord({5, 6, 7, 8}));

    const auto statsAfterFirst = backend.getBackendStats();
    ASSERT_TRUE(statsAfterFirst.has_value());
    const std::uint64_t writtenAfterFirst = statsAfterFirst->bytesWritten;
    EXPECT_GT(writtenAfterFirst, 0U);

    /// Second batch — additional writes must advance the counter.
    for (int i = 3; i <= 10; ++i)
    {
        backend.put(SliceEnd{Timestamp(static_cast<uint64_t>(i))}, WorkerThreadId{0}, makeRecord({0xFF, 0xFE, 0xFD}));
    }

    const auto statsAfterMore = backend.getBackendStats();
    ASSERT_TRUE(statsAfterMore.has_value());
    EXPECT_GT(statsAfterMore->bytesWritten, writtenAfterFirst)
        << "bytesWritten must increase after additional puts";
}

/// E1-PR2 (c): NullSpillBackend and InMemorySpillBackend return nullopt from
/// getBackendStats() — they inherit the base default and do not collect stats.
///
/// Why RED on baseline: SpillBackend::getBackendStats() does not exist; both
/// calls fail at compile time.
TEST_F(RocksDBSpillBackendTest, nullAndInMemoryBackendsReturnNulloptStats)
{
    /// Test via the base interface pointer to exercise the virtual dispatch path.
    const std::unique_ptr<SpillBackend> nullBackend = std::make_unique<NullSpillBackend>();
    EXPECT_FALSE(nullBackend->getBackendStats().has_value())
        << "NullSpillBackend must return nullopt from getBackendStats()";

    const std::unique_ptr<SpillBackend> inMemBackend = std::make_unique<InMemorySpillBackend>();
    EXPECT_FALSE(inMemBackend->getBackendStats().has_value())
        << "InMemorySpillBackend must return nullopt from getBackendStats()";
}

// ============================================================================
// H2: write-stall fields — writeStallMicros + writeStallCount
// ----------------------------------------------------------------------------
// These tests verify that BackendStats exposes the two new H2 fields and that
// they satisfy the invariants required for hypothesis testing:
//   1. The fields are present and readable (not a compile error).
//   2. writeStallMicros is non-decreasing across additional puts.
//   3. writeStallCount is non-decreasing across additional puts.
//   4. Both fields are 0 when no stalls occurred (valid outcome for H2: "< 5%").
//
// Note: in a short unit-test run RocksDB will almost certainly NOT stall
// (stalls require the write path to outpace compaction under sustained load).
// The tests therefore assert >= 0 (the field is present and non-negative) and
// monotonicity (non-decreasing), NOT a specific non-zero value.  A non-zero
// value from a longer / memory-constrained production run is what the eval
// harness uses to compute the stall fraction for H2.
// ============================================================================

/// H2 (a): getBackendStats() returns the writeStallMicros and writeStallCount
/// fields after writes; both are >= 0 (unsigned — always true, but the test
/// documents the field presence contract explicitly).
///
/// GREEN contract:
///   - stats is non-nullopt.
///   - writeStallMicros >= 0 (field exists; 0 = no stalls — valid for H2).
///   - writeStallCount  >= 0 (field exists; 0 = no stall events — valid).
///   - writeStallMicros == 0 implies writeStallCount == 0 (duration-count
///     consistency: you cannot have stall events without stall time).
TEST_F(RocksDBSpillBackendTest, writeStallFieldsPresentAfterWrites)
{
    RocksDBSpillBackend backend(dbPath.string());

    constexpr int NUM_RECORDS = 30;
    for (int i = 0; i < NUM_RECORDS; ++i)
    {
        backend.put(
            SliceEnd{Timestamp(static_cast<uint64_t>(i + 1))},
            WorkerThreadId{0},
            makeRecord({0xDE, 0xAD, 0xBE, 0xEF}));
    }

    const auto stats = backend.getBackendStats();
    ASSERT_TRUE(stats.has_value()) << "RocksDBSpillBackend must return a BackendStats";

    /// Both fields are std::uint64_t — they satisfy >= 0 by type; the assertion
    /// documents that the fields are readable and are the expected unsigned type.
    /// EXPECT_GE on uint64_t with 0U prevents signed/unsigned comparison warnings.
    EXPECT_GE(stats->writeStallMicros, 0U)
        << "writeStallMicros must be a non-negative cumulative duration";
    EXPECT_GE(stats->writeStallCount, 0U)
        << "writeStallCount must be a non-negative event count";

    /// Duration-count consistency: stall events require non-zero duration and vice versa.
    /// If stall count > 0, there must have been some stall time; if stall time > 0,
    /// at least one stall event must have been recorded.
    if (stats->writeStallMicros == 0)
    {
        EXPECT_EQ(stats->writeStallCount, 0U)
            << "writeStallCount must be 0 when writeStallMicros == 0 (no duration, no events)";
    }
    if (stats->writeStallCount == 0)
    {
        EXPECT_EQ(stats->writeStallMicros, 0U)
            << "writeStallMicros must be 0 when writeStallCount == 0 (no events, no duration)";
    }
}

/// H2 (b): writeStallMicros and writeStallCount are non-decreasing (monotone)
/// as more puts are issued.  RocksDB tickers and histogram counts never decrease
/// within a DB lifetime; this test verifies the property over two snapshots.
///
/// GREEN contract:
///   - statsAfterMore->writeStallMicros >= statsAfterFirst->writeStallMicros
///   - statsAfterMore->writeStallCount  >= statsAfterFirst->writeStallCount
TEST_F(RocksDBSpillBackendTest, writeStallFieldsAreNonDecreasingAcrossMorePuts)
{
    RocksDBSpillBackend backend(dbPath.string());

    /// First batch — snapshot the stall counters.
    for (int i = 0; i < 5; ++i)
    {
        backend.put(
            SliceEnd{Timestamp(static_cast<uint64_t>(i + 1))},
            WorkerThreadId{0},
            makeRecord({0x01, 0x02}));
    }
    const auto statsAfterFirst = backend.getBackendStats();
    ASSERT_TRUE(statsAfterFirst.has_value());
    const std::uint64_t stallMicrosAfterFirst = statsAfterFirst->writeStallMicros;
    const std::uint64_t stallCountAfterFirst = statsAfterFirst->writeStallCount;

    /// Second batch — counters must be >= the first snapshot (monotone).
    for (int i = 5; i < 20; ++i)
    {
        backend.put(
            SliceEnd{Timestamp(static_cast<uint64_t>(i + 1))},
            WorkerThreadId{0},
            makeRecord({0xAA, 0xBB, 0xCC}));
    }
    const auto statsAfterMore = backend.getBackendStats();
    ASSERT_TRUE(statsAfterMore.has_value());

    EXPECT_GE(statsAfterMore->writeStallMicros, stallMicrosAfterFirst)
        << "writeStallMicros must be non-decreasing (RocksDB STALL_MICROS ticker never resets)";
    EXPECT_GE(statsAfterMore->writeStallCount, stallCountAfterFirst)
        << "writeStallCount must be non-decreasing (WRITE_STALL histogram count never resets)";
}

/// H2 (c): the Null/InMemory backends continue to return nullopt — the H2 fields
/// in BackendStats must not break the existing nullopt contract for non-RocksDB
/// backends.  Keeps E1-PR2 test (c) intact while confirming H2 fields compile.
///
/// This test is structurally identical to nullAndInMemoryBackendsReturnNulloptStats
/// but is named for H2 to make the regression intent explicit in the test report.
TEST_F(RocksDBSpillBackendTest, writeStallFieldsAbsentForNullAndInMemoryBackends)
{
    const std::unique_ptr<SpillBackend> nullBackend = std::make_unique<NullSpillBackend>();
    EXPECT_FALSE(nullBackend->getBackendStats().has_value())
        << "NullSpillBackend must still return nullopt after H2 fields were added to BackendStats";

    const std::unique_ptr<SpillBackend> inMemBackend = std::make_unique<InMemorySpillBackend>();
    EXPECT_FALSE(inMemBackend->getBackendStats().has_value())
        << "InMemorySpillBackend must still return nullopt after H2 fields were added to BackendStats";
}

}
